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
from viam.utils import ValueTypes


class Worker(Generic, EasyResource):
    """Worker component that sweeps orphaned directories from decommissioned resources."""
    
    MODEL: ClassVar[Model] = Model(ModelFamily("hunter", "disk-sweeper"), "worker")
    
    @classmethod
    def new(
        cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ) -> Self:
        """This method creates a new instance of this Worker component.
        
        Args:
            config (ComponentConfig): The configuration for this resource
            dependencies (Mapping[ResourceName, ResourceBase]): The dependencies
            
        Returns:
            Self: The resource
        """
        return super().new(config, dependencies)
    
    @classmethod
    def validate_config(
        cls, config: ComponentConfig
    ) -> Tuple[Sequence[str], Sequence[str]]:
        """This method validates the configuration object received from the machine.
        
        Args:
            config (ComponentConfig): The configuration for this resource
            
        Returns:
            Tuple[Sequence[str], Sequence[str]]: A tuple of (required_deps, optional_deps)
        """
        return [], []  # No dependencies needed
    
    def reconfigure(
        self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ):
        """This method allows dynamic updates when receiving a new config.
        
        Args:
            config (ComponentConfig): The new configuration
            dependencies (Mapping[ResourceName, ResourceBase]): Any dependencies
        """
        # Parse attributes safely
        attrs = {}
        if hasattr(config, 'attributes') and config.attributes:
            if hasattr(config.attributes, 'fields') and config.attributes.fields:
                json_field = config.attributes.fields.get("json", None)
                if json_field and hasattr(json_field, 'string_value'):
                    try:
                        attrs = json.loads(json_field.string_value)
                    except json.JSONDecodeError:
                        self.logger.warning("Failed to parse config attributes")
        
        # Apply configuration with defaults
        self.target_path = Path(attrs.get("target_path", "/root/.viam/video-storage"))
        self.days_old = attrs.get("days_old", 7)
        self.dry_run = attrs.get("dry_run", True)
        
        self.logger.info(
            f"Worker configured: path={self.target_path}, "
            f"days_old={self.days_old}, dry_run={self.dry_run}"
        )
        
        return super().reconfigure(config, dependencies)
    
    async def do_command(
        self,
        command: Mapping[str, ValueTypes],
        *,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Mapping[str, ValueTypes]:
        """Execute worker commands: status, analyze, or sweep.
        
        Args:
            command: Command dictionary with 'command' key
            timeout: Optional timeout
            
        Returns:
            Command results dictionary
        """
        cmd = command.get("command", "status")
        
        self.logger.debug(f"Executing command: {cmd}")
        
        if cmd == "status":
            return self._get_status()
        elif cmd == "analyze":
            return self._analyze()
        elif cmd == "sweep":
            return self._sweep()
        else:
            error_msg = f"Unknown command: '{cmd}'. Valid commands: status, analyze, sweep"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
    
    async def get_geometries(
        self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
    ) -> List[Geometry]:
        """Get geometries (not implemented for this component)."""
        return []
    
    def _get_status(self) -> Dict[str, Any]:
        """Get current status and configuration."""
        status = {
            "target_path": str(self.target_path),
            "days_old": self.days_old,
            "dry_run": self.dry_run,
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
    
    def _get_active_resources(self) -> List[str]:
        """Extract active resource names from machine configuration."""
        config_dir = Path("/root/.viam")
        
        try:
            # Find all config files
            config_files = list(config_dir.glob("cached_cloud_config_*.json"))
            
            if not config_files:
                self.logger.warning("No cached config files found")
                return []
            
            # Use most recent config
            latest = max(config_files, key=lambda f: f.stat().st_mtime)
            self.logger.debug(f"Reading config from: {latest.name}")
            
            with open(latest, 'r') as f:
                config = json.load(f)
            
            # Extract component names
            resources = []
            for component in config.get("components", []):
                name = component.get("name", "")
                if name:
                    resources.append(name)
            
            self.logger.info(f"Found {len(resources)} active resources")
            return resources
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse config JSON: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to read machine config: {e}")
            return []
    
    def _analyze(self) -> Dict[str, Any]:
        """Analyze directories and identify cleanup candidates."""
        if not self.target_path.exists():
            return {
                "error": f"Target path does not exist: {self.target_path}",
                "active_resources": [],
                "orphaned_directories": [],
                "eligible_for_cleanup": 0,
                "total_orphans": 0,
                "recoverable_mb": 0
            }
        
        active = set(self._get_active_resources())
        orphans = []
        
        try:
            for item in self.target_path.iterdir():
                if not item.is_dir():
                    continue
                
                # Check if orphaned
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
                "active_resources": list(active),
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
            "active_resources": list(active),
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