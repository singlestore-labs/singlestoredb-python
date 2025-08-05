#!/usr/bin/env python
"""SingleStoreDB Metrics Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

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

    @property
    def latest_value(self) -> Optional[Union[int, float]]:
        """
        Get the latest value from the data points.

        Returns
        -------
        int or float or None
            Latest metric value, or None if no data points exist

        Examples
        --------
        >>> workspace_group = workspace_manager.get_workspace_group("wg-123")
        >>> metrics = workspace_group.get_metrics()
        >>> cpu_metric = metrics["cpu_usage"]
        >>> latest_cpu = cpu_metric.latest_value
        >>> print(f"Latest CPU usage: {latest_cpu}%")

        """
        if not self.data_points:
            return None

        # Assuming data points are sorted by timestamp
        return self.data_points[-1].value

    @property
    def average_value(self) -> Optional[float]:
        """
        Get the average value from all data points.

        Returns
        -------
        float or None
            Average metric value, or None if no data points exist

        Examples
        --------
        >>> workspace_group = workspace_manager.get_workspace_group("wg-123")
        >>> metrics = workspace_group.get_metrics()
        >>> cpu_metric = metrics["cpu_usage"]
        >>> avg_cpu = cpu_metric.average_value
        >>> print(f"Average CPU usage: {avg_cpu:.2f}%")

        """
        if not self.data_points:
            return None

        total = sum(dp.value for dp in self.data_points)
        return total / len(self.data_points)

    @property
    def max_value(self) -> Optional[Union[int, float]]:
        """
        Get the maximum value from all data points.

        Returns
        -------
        int or float or None
            Maximum metric value, or None if no data points exist

        Examples
        --------
        >>> workspace_group = workspace_manager.get_workspace_group("wg-123")
        >>> metrics = workspace_group.get_metrics()
        >>> cpu_metric = metrics["cpu_usage"]
        >>> max_cpu = cpu_metric.max_value
        >>> print(f"Peak CPU usage: {max_cpu}%")

        """
        if not self.data_points:
            return None

        return max(dp.value for dp in self.data_points)

    @property
    def min_value(self) -> Optional[Union[int, float]]:
        """
        Get the minimum value from all data points.

        Returns
        -------
        int or float or None
            Minimum metric value, or None if no data points exist

        Examples
        --------
        >>> workspace_group = workspace_manager.get_workspace_group("wg-123")
        >>> metrics = workspace_group.get_metrics()
        >>> cpu_metric = metrics["cpu_usage"]
        >>> min_cpu = cpu_metric.min_value
        >>> print(f"Minimum CPU usage: {min_cpu}%")

        """
        if not self.data_points:
            return None

        return min(dp.value for dp in self.data_points)
