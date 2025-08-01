#!/usr/bin/env python
"""SingleStoreDB Metrics Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .manager import Manager
from .utils import to_datetime
from .utils import vars_to_str


class MetricDataPoint(object):
    """
    A single metric data point.

    This object represents a single measurement value at a specific timestamp.
    """

    def __init__(
        self,
        timestamp: Union[str, datetime.datetime],
        value: Union[int, float],
        unit: Optional[str] = None,
    ):
        #: Timestamp of the measurement
        self.timestamp = to_datetime(timestamp)

        #: Value of the measurement
        self.value = value

        #: Unit of measurement
        self.unit = unit

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'MetricDataPoint':
        """
        Construct a MetricDataPoint from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`MetricDataPoint`
        """
        return cls(
            timestamp=obj['timestamp'],
            value=obj['value'],
            unit=obj.get('unit'),
        )


class WorkspaceGroupMetric(object):
    """
    Workspace group metric definition.

    This object represents a metric for a workspace group, containing
    metadata about the metric and its data points.
    """

    def __init__(
        self,
        metric_name: str,
        metric_type: str,
        description: Optional[str] = None,
        unit: Optional[str] = None,
        data_points: Optional[List[MetricDataPoint]] = None,
        workspace_group_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        aggregation_type: Optional[str] = None,
    ):
        #: Name of the metric
        self.metric_name = metric_name

        #: Type of metric (e.g., 'counter', 'gauge', 'histogram')
        self.metric_type = metric_type

        #: Description of what the metric measures
        self.description = description

        #: Unit of measurement
        self.unit = unit

        #: List of data points for this metric
        self.data_points = data_points or []

        #: Workspace group ID this metric belongs to
        self.workspace_group_id = workspace_group_id

        #: Workspace ID this metric belongs to (if workspace-specific)
        self.workspace_id = workspace_id

        #: Type of aggregation applied to the metric
        self.aggregation_type = aggregation_type

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'WorkspaceGroupMetric':
        """
        Construct a WorkspaceGroupMetric from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`WorkspaceGroupMetric`
        """
        data_points = []
        if 'dataPoints' in obj:
            data_points = [
                MetricDataPoint.from_dict(dp)
                for dp in obj['dataPoints']
            ]

        return cls(
            metric_name=obj['metricName'],
            metric_type=obj['metricType'],
            description=obj.get('description'),
            unit=obj.get('unit'),
            data_points=data_points,
            workspace_group_id=obj.get('workspaceGroupID'),
            workspace_id=obj.get('workspaceID'),
            aggregation_type=obj.get('aggregationType'),
        )

    def get_latest_value(self) -> Optional[Union[int, float]]:
        """
        Get the latest value from the data points.

        Returns
        -------
        int or float or None
            Latest metric value, or None if no data points exist

        Examples
        --------
        >>> metric = metrics_mgr.get_workspace_group_metrics("wg-123")["cpu_usage"]
        >>> latest_cpu = metric.get_latest_value()
        >>> print(f"Latest CPU usage: {latest_cpu}%")
        """
        if not self.data_points:
            return None

        # Assuming data points are sorted by timestamp
        return self.data_points[-1].value

    def get_average_value(self) -> Optional[float]:
        """
        Get the average value from all data points.

        Returns
        -------
        float or None
            Average metric value, or None if no data points exist

        Examples
        --------
        >>> metric = metrics_mgr.get_workspace_group_metrics("wg-123")["cpu_usage"]
        >>> avg_cpu = metric.get_average_value()
        >>> print(f"Average CPU usage: {avg_cpu:.2f}%")
        """
        if not self.data_points:
            return None

        total = sum(dp.value for dp in self.data_points)
        return total / len(self.data_points)

    def get_max_value(self) -> Optional[Union[int, float]]:
        """
        Get the maximum value from all data points.

        Returns
        -------
        int or float or None
            Maximum metric value, or None if no data points exist
        """
        if not self.data_points:
            return None

        return max(dp.value for dp in self.data_points)

    def get_min_value(self) -> Optional[Union[int, float]]:
        """
        Get the minimum value from all data points.

        Returns
        -------
        int or float or None
            Minimum metric value, or None if no data points exist
        """
        if not self.data_points:
            return None

        return min(dp.value for dp in self.data_points)


class MetricsManager(Manager):
    """
    SingleStoreDB metrics manager.

    This class should be instantiated using :func:`singlestoredb.manage_metrics`
    or accessed via :attr:`WorkspaceManager.metrics`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use (defaults to 'v2' for metrics)
    base_url : str, optional
        Base URL of the management API
    """

    #: Object type
    obj_type = 'metrics'

    #: Default version for metrics API
    default_version = 'v2'

    def get_workspace_group_metrics(
        self,
        organization_id: str,
        workspace_group_id: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        metric_names: Optional[List[str]] = None,
        workspace_id: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> Dict[str, WorkspaceGroupMetric]:
        """
        Get metrics for a workspace group.

        Parameters
        ----------
        organization_id : str
            ID of the organization
        workspace_group_id : str
            ID of the workspace group
        start_time : datetime.datetime, optional
            Start time for metrics data
        end_time : datetime.datetime, optional
            End time for metrics data
        metric_names : List[str], optional
            List of specific metric names to retrieve
        workspace_id : str, optional
            ID of specific workspace to get metrics for
        aggregation_type : str, optional
            Type of aggregation ('avg', 'sum', 'max', 'min')
        resolution : str, optional
            Time resolution for data points ('1m', '5m', '1h', '1d')

        Returns
        -------
        Dict[str, WorkspaceGroupMetric]
            Dictionary mapping metric names to metric objects

        Examples
        --------
        >>> metrics_mgr = singlestoredb.manage_metrics()
        >>> metrics = metrics_mgr.get_workspace_group_metrics(
        ...     organization_id="org-123",
        ...     workspace_group_id="wg-456",
        ...     start_time=datetime.datetime.now() - datetime.timedelta(hours=24),
        ...     metric_names=["cpu_usage", "memory_usage", "storage_usage"]
        ... )
        >>>
        >>> for name, metric in metrics.items():
        ...     print(f"{name}: {metric.get_latest_value()} {metric.unit}")
        """
        params = {}

        if start_time:
            params['startTime'] = start_time.isoformat()
        if end_time:
            params['endTime'] = end_time.isoformat()
        if metric_names:
            params['metricNames'] = ','.join(metric_names)
        if workspace_id:
            params['workspaceID'] = workspace_id
        if aggregation_type:
            params['aggregationType'] = aggregation_type
        if resolution:
            params['resolution'] = resolution

        path = (
            f'organizations/{organization_id}/workspaceGroups/'
            f'{workspace_group_id}/metrics'
        )
        res = self._get(path, params=params if params else None)

        metrics_data = res.json()
        metrics_dict = {}

        # Handle different possible response structures
        if isinstance(metrics_data, list):
            for metric_obj in metrics_data:
                metric = WorkspaceGroupMetric.from_dict(metric_obj)
                metrics_dict[metric.metric_name] = metric
        elif isinstance(metrics_data, dict):
            if 'metrics' in metrics_data:
                for metric_obj in metrics_data['metrics']:
                    metric = WorkspaceGroupMetric.from_dict(metric_obj)
                    metrics_dict[metric.metric_name] = metric
            else:
                # Assume the dict itself contains metric data
                for name, data in metrics_data.items():
                    if isinstance(data, dict):
                        data['metricName'] = name
                        metric = WorkspaceGroupMetric.from_dict(data)
                        metrics_dict[name] = metric

        return metrics_dict

    def get_cpu_metrics(
        self,
        organization_id: str,
        workspace_group_id: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        workspace_id: Optional[str] = None,
    ) -> Optional[WorkspaceGroupMetric]:
        """
        Get CPU usage metrics for a workspace group.

        Parameters
        ----------
        organization_id : str
            ID of the organization
        workspace_group_id : str
            ID of the workspace group
        start_time : datetime.datetime, optional
            Start time for metrics data
        end_time : datetime.datetime, optional
            End time for metrics data
        workspace_id : str, optional
            ID of specific workspace to get metrics for

        Returns
        -------
        WorkspaceGroupMetric or None
            CPU usage metric, or None if not available

        Examples
        --------
        >>> metrics_mgr = singlestoredb.manage_metrics()
        >>> cpu_metric = metrics_mgr.get_cpu_metrics("org-123", "wg-456")
        >>> if cpu_metric:
        ...     print(f"Current CPU usage: {cpu_metric.get_latest_value()}%")
        """
        metrics = self.get_workspace_group_metrics(
            organization_id=organization_id,
            workspace_group_id=workspace_group_id,
            start_time=start_time,
            end_time=end_time,
            metric_names=['cpu_usage', 'cpu_utilization'],
            workspace_id=workspace_id,
        )

        # Try common CPU metric names
        for name in ['cpu_usage', 'cpu_utilization', 'cpu']:
            if name in metrics:
                return metrics[name]

        return None

    def get_memory_metrics(
        self,
        organization_id: str,
        workspace_group_id: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        workspace_id: Optional[str] = None,
    ) -> Optional[WorkspaceGroupMetric]:
        """
        Get memory usage metrics for a workspace group.

        Parameters
        ----------
        organization_id : str
            ID of the organization
        workspace_group_id : str
            ID of the workspace group
        start_time : datetime.datetime, optional
            Start time for metrics data
        end_time : datetime.datetime, optional
            End time for metrics data
        workspace_id : str, optional
            ID of specific workspace to get metrics for

        Returns
        -------
        WorkspaceGroupMetric or None
            Memory usage metric, or None if not available

        Examples
        --------
        >>> metrics_mgr = singlestoredb.manage_metrics()
        >>> memory_metric = metrics_mgr.get_memory_metrics("org-123", "wg-456")
        >>> if memory_metric:
        ...     print(f"Current memory usage: {memory_metric.get_latest_value()} MB")
        """
        metrics = self.get_workspace_group_metrics(
            organization_id=organization_id,
            workspace_group_id=workspace_group_id,
            start_time=start_time,
            end_time=end_time,
            metric_names=['memory_usage', 'memory_utilization'],
            workspace_id=workspace_id,
        )

        # Try common memory metric names
        for name in ['memory_usage', 'memory_utilization', 'memory']:
            if name in metrics:
                return metrics[name]

        return None

    def get_storage_metrics(
        self,
        organization_id: str,
        workspace_group_id: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        workspace_id: Optional[str] = None,
    ) -> Optional[WorkspaceGroupMetric]:
        """
        Get storage usage metrics for a workspace group.

        Parameters
        ----------
        organization_id : str
            ID of the organization
        workspace_group_id : str
            ID of the workspace group
        start_time : datetime.datetime, optional
            Start time for metrics data
        end_time : datetime.datetime, optional
            End time for metrics data
        workspace_id : str, optional
            ID of specific workspace to get metrics for

        Returns
        -------
        WorkspaceGroupMetric or None
            Storage usage metric, or None if not available

        Examples
        --------
        >>> metrics_mgr = singlestoredb.manage_metrics()
        >>> storage_metric = metrics_mgr.get_storage_metrics("org-123", "wg-456")
        >>> if storage_metric:
        ...     print(f"Current storage usage: {storage_metric.get_latest_value()} GB")
        """
        metrics = self.get_workspace_group_metrics(
            organization_id=organization_id,
            workspace_group_id=workspace_group_id,
            start_time=start_time,
            end_time=end_time,
            metric_names=['storage_usage', 'disk_usage'],
            workspace_id=workspace_id,
        )

        # Try common storage metric names
        for name in ['storage_usage', 'disk_usage', 'storage']:
            if name in metrics:
                return metrics[name]

        return None


def manage_metrics(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> MetricsManager:
    """
    Retrieve a SingleStoreDB metrics manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use (defaults to 'v2' for metrics)
    base_url : str, optional
        Base URL of the management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`MetricsManager`

    Examples
    --------
    >>> import singlestoredb as s2
    >>> metrics_mgr = s2.manage_metrics()
    >>> metrics = metrics_mgr.get_workspace_group_metrics(
    ...     organization_id="org-123",
    ...     workspace_group_id="wg-456"
    ... )
    >>> print(f"Retrieved {len(metrics)} metrics")
    """
    return MetricsManager(
        access_token=access_token,
        base_url=base_url,
        version=version or 'v2',
        organization_id=organization_id,
    )
