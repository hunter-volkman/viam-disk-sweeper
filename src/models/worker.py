import json
import shutil
from pathlib import Path
from typing import (Any, ClassVar, Dict, Final, List, Mapping, Optional,
                    Sequence, Tuple)
from datetime import datetime

from typing_extensions import Self
from viam.components.generic import Generic
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import Geometry, ResourceName
from viam.resource.base import ResourceBase
from viam.resource.easy_resource import EasyResource
from viam.resource.types import Model, ModelFamily
from viam.utils import ValueTypes, struct_to_dict


class Worker(Generic, EasyResource):
    """Worker component that sweeps orphaned directories from decommissioned resources."""
    
    MODEL: ClassVar[Model] = Model(ModelFamily("hunter", "disk-sweeper"), "worker")
    
    @classmethod
    def new(
        cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ) -> Self:
        """Create a new Worker instance."""
        return super().new(config, dependencies)
    
    @classmethod
    def validate_config(
        cls, config: ComponentConfig
    ) -> Tuple[Sequence[str], Sequence[str]]:
        """Validate configuration."""
        return [], []
    
    def reconfigure(
        self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ):
        """Apply configuration updates."""
        # Parse attributes from config
        attrs = struct_to_dict(config.attributes)

        self.logger.info(f"PARSED ATTRS: {attrs}")
        
        # Required configuration
        self.target_path = Path(attrs.get("target_path", "/root/.viam/video-storage"))
        
        # Optional configuration with defaults
        self.days_old = attrs.get("days_old", 7)
        self.dry_run = attrs.get("dry_run", True)
        
        # Accept list of active components in config for determining any orphans
        self.active_components = attrs.get("active_components", [])
        
        self.logger.info(
            f"Worker configured: path={self.target_path}, "
            f"days_old={self.days_old}, dry_run={self.dry_run}, "
            f"active_components={len(self.active_components)}"
        )
    
    async def do_command(
        self,
        command: Mapping[str, ValueTypes],
        *,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Mapping[str, ValueTypes]:
        """Execute worker commands."""
        cmd = command.get("command", "status")
        
        self.logger.debug(f"Executing command: {cmd}")
        
        if cmd == "status":
            return self._get_status()
        elif cmd == "analyze":
            return self._analyze()
        elif cmd == "sweep":
            return self._sweep()
        else:
            raise ValueError(f"Unknown command: '{cmd}'. Valid commands: status, analyze, sweep")
    
    async def get_geometries(
        self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
    ) -> List[Geometry]:
        """Get geometries (not implemented)."""
        return []
    
    def _get_status(self) -> Dict[str, Any]:
        """Get current status and configuration."""
        status = {
            "target_path": str(self.target_path),
            "days_old": self.days_old,
            "dry_run": self.dry_run,
            "active_components": len(self.active_components),
            "exists": self.target_path.exists()
        }
        
        if self.target_path.exists():
            try:
                dirs = [d for d in self.target_path.iterdir() if d.is_dir()]
                total_size = 0
                for d in dirs:
                    try:
                        size = sum(f.stat().st_size for f in d.rglob('*') if f.is_file())
                        total_size += size
                    except Exception:
                        pass
                
                status["directory_count"] = len(dirs)
                status["total_size_mb"] = round(total_size / (1024 * 1024), 2)
            except Exception as e:
                self.logger.warning(f"Could not get directory statistics: {e}")
                status["directory_count"] = 0
                status["total_size_mb"] = 0
        else:
            status["directory_count"] = 0
            status["total_size_mb"] = 0
        
        return status
    
    def _analyze(self) -> Dict[str, Any]:
        """Analyze directories and identify cleanup candidates."""
        if not self.target_path.exists():
            return {
                "error": f"Target path does not exist: {self.target_path}",
                "active_resources": self.active_components,
                "orphaned_directories": [],
                "eligible_for_cleanup": 0,
                "total_orphans": 0,
                "recoverable_mb": 0
            }
        
        # Use configured active components
        active = set(self.active_components)
        orphans = []
        
        try:
            for item in self.target_path.iterdir():
                if not item.is_dir():
                    continue
                
                # Directory is orphaned if not in active components
                if item.name not in active:
                    try:
                        stat = item.stat()
                        mtime = datetime.fromtimestamp(stat.st_mtime)
                        age = datetime.now() - mtime
                        
                        # Calculate size
                        size = 0
                        for f in item.rglob('*'):
                            if f.is_file():
                                try:
                                    size += f.stat().st_size
                                except:
                                    pass
                        
                        orphan_info = {
                            "name": item.name,
                            "age_days": age.days,
                            "size_mb": round(size / (1024 * 1024), 2),
                            "eligible": age.days >= self.days_old
                        }
                        
                        orphans.append(orphan_info)
                        
                    except Exception as e:
                        self.logger.warning(f"Could not analyze {item.name}: {e}")
            
        except Exception as e:
            self.logger.error(f"Failed to scan directory: {e}")
            return {
                "error": f"Failed to scan: {e}",
                "active_resources": self.active_components,
                "orphaned_directories": [],
                "eligible_for_cleanup": 0,
                "total_orphans": 0,
                "recoverable_mb": 0
            }
        
        # Calculate summary
        eligible = [o for o in orphans if o["eligible"]]
        total_recoverable = sum(o["size_mb"] for o in eligible)
        
        # Sort by size (largest first)
        orphans.sort(key=lambda x: x["size_mb"], reverse=True)
        
        return {
            "active_resources": self.active_components,
            "orphaned_directories": orphans,
            "eligible_for_cleanup": len(eligible),
            "total_orphans": len(orphans),
            "recoverable_mb": round(total_recoverable, 2)
        }
    
    def _sweep(self) -> Dict[str, Any]:
        """Execute cleanup of orphaned directories."""
        analysis = self._analyze()
        
        if "error" in analysis:
            return {"error": analysis["error"]}
        
        deleted = []
        skipped = []
        errors = []
        freed_mb = 0
        
        for orphan in analysis["orphaned_directories"]:
            if not orphan["eligible"]:
                skipped.append({
                    "name": orphan["name"],
                    "reason": f"Only {orphan['age_days']} days old (threshold: {self.days_old})"
                })
                continue
            
            orphan_path = self.target_path / orphan["name"]
            
            try:
                if self.dry_run:
                    self.logger.info(
                        f"[DRY RUN] Would delete {orphan['name']} "
                        f"({orphan['size_mb']} MB, {orphan['age_days']} days old)"
                    )
                    deleted.append(orphan["name"])
                    freed_mb += orphan["size_mb"]
                else:
                    shutil.rmtree(orphan_path)
                    self.logger.info(
                        f"Deleted {orphan['name']} "
                        f"({orphan['size_mb']} MB, {orphan['age_days']} days old)"
                    )
                    deleted.append(orphan["name"])
                    freed_mb += orphan["size_mb"]
                    
            except PermissionError as e:
                error_msg = f"Permission denied for {orphan['name']}"
                self.logger.error(f"{error_msg}: {e}")
                errors.append({"name": orphan["name"], "error": error_msg})
            except Exception as e:
                error_msg = f"Failed to delete {orphan['name']}"
                self.logger.error(f"{error_msg}: {e}")
                errors.append({"name": orphan["name"], "error": str(e)})
        
        if deleted:
            self.logger.info(
                f"Sweep complete: deleted {len(deleted)} directories, "
                f"freed {freed_mb} MB (dry_run={self.dry_run})"
            )
        
        return {
            "deleted": deleted,
            "skipped": skipped,
            "errors": errors,
            "freed_mb": round(freed_mb, 2),
            "dry_run": self.dry_run
        }