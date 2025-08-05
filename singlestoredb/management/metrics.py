#!/usr/bin/env python
"""SingleStoreDB Metrics Management."""
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .manager import Manager


class MetricDataPoint(object):
    """
    A single metric data point from OpenMetrics format.

    This object represents a single measurement value with labels.

    """

    def __init__(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ):
        #: Name of the metric
        self.metric_name = metric_name

        #: Value of the measurement
        self.value = value

        #: Labels associated with this metric
        self.labels = labels or {}

    def __str__(self) -> str:
        """Return string representation."""
        labels_str = ','.join(f'{k}="{v}"' for k, v in self.labels.items())
        return f'{self.metric_name}{{{labels_str}}} {self.value}'

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class WorkspaceGroupMetrics(object):
    """
    Workspace group metrics definition.

    This object represents metrics for a workspace group, containing
    parsed OpenMetrics data.

    """

    def __init__(
        self,
        workspace_group_id: str,
        raw_metrics: str,
        data_points: Optional[List[MetricDataPoint]] = None,
    ):
        #: Workspace group ID these metrics belong to
        self.workspace_group_id = workspace_group_id

        #: Raw OpenMetrics text response
        self.raw_metrics = raw_metrics

        #: Parsed metric data points
        self.data_points = data_points or []

    def __str__(self) -> str:
        """Return string representation."""
        return (
            f'WorkspaceGroupMetrics(workspace_group_id={self.workspace_group_id}, '
            f'data_points={len(self.data_points)})'
        )

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_openmetrics_text(
        cls,
        workspace_group_id: str,
        metrics_text: str,
    ) -> 'WorkspaceGroupMetrics':
        """
        Parse OpenMetrics text format into structured data.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group
        metrics_text : str
            Raw OpenMetrics text response

        Returns
        -------
        :class:`WorkspaceGroupMetrics`

        """
        data_points = []

        # Parse OpenMetrics format
        # Example: singlestoredb_cloud_threads_running{extractor="...",node="..."} 1
        pattern = r'([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+([0-9.-]+)'

        for line in metrics_text.split('\n'):
            line = line.strip()
            if line.startswith('#') or not line:
                continue

            match = re.match(pattern, line)
            if match:
                metric_name = match.group(1)
                labels_str = match.group(2)
                value = float(match.group(3))

                # Parse labels
                labels = {}
                if labels_str:
                    # Parse label=value pairs
                    label_pattern = r'([^=,]+)="([^"]*)"'
                    for label_match in re.finditer(label_pattern, labels_str):
                        key = label_match.group(1).strip()
                        val = label_match.group(2)
                        labels[key] = val

                data_points.append(
                    MetricDataPoint(
                        metric_name=metric_name,
                        value=value,
                        labels=labels,
                    ),
                )

        return cls(
            workspace_group_id=workspace_group_id,
            raw_metrics=metrics_text,
            data_points=data_points,
        )

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'WorkspaceGroupMetrics':
        """
        Construct a WorkspaceGroupMetrics from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values containing metric data

        Returns
        -------
        :class:`WorkspaceGroupMetrics`

        """
        workspace_group_id = obj.get('workspaceGroupId', '')
        metric_name = obj.get('metricName', '')

        # Convert dict data to data points
        data_points = []
        if 'dataPoints' in obj:
            for dp in obj['dataPoints']:
                data_points.append(
                    MetricDataPoint(
                        metric_name=metric_name,
                        value=float(dp.get('value', 0)),
                        labels=dp.get('labels', {}),
                    ),
                )
        elif 'value' in obj:
            # Single data point
            data_points.append(
                MetricDataPoint(
                    metric_name=metric_name,
                    value=float(obj['value']),
                    labels=obj.get('labels', {}),
                ),
            )

        return cls(
            workspace_group_id=workspace_group_id,
            raw_metrics='',  # No raw metrics for JSON data
            data_points=data_points,
        )

    def get_metrics_by_name(self, metric_name: str) -> List[MetricDataPoint]:
        """
        Get all data points for a specific metric name.

        Parameters
        ----------
        metric_name : str
            Name of the metric to filter by

        Returns
        -------
        List[MetricDataPoint]
            List of data points matching the metric name

        Examples
        --------
        >>> metrics = workspace_group.get_metrics()
        >>> cpu_metrics = metrics.get_metrics_by_name(
        ...     "singlestoredb_cloud_cpu_usage"
        ... )
        >>> for point in cpu_metrics:
        ...     print(f"Node {point.labels.get('node')}: {point.value}%")

        """
        return [
            dp for dp in self.data_points if dp.metric_name == metric_name
        ]

    def get_metrics_by_label(
        self, label_key: str, label_value: str,
    ) -> List[MetricDataPoint]:
        """
        Get all data points that have a specific label value.

        Parameters
        ----------
        label_key : str
            Label key to filter by
        label_value : str
            Label value to filter by

        Returns
        -------
        List[MetricDataPoint]
            List of data points matching the label

        Examples
        --------
        >>> metrics = workspace_group.get_metrics()
        >>> node_metrics = metrics.get_metrics_by_label("node", "aggregator-0")
        >>> for point in node_metrics:
        ...     print(f"{point.metric_name}: {point.value}")

        """
        return [
            dp for dp in self.data_points
            if dp.labels.get(label_key) == label_value
        ]

    @property
    def metric_names(self) -> List[str]:
        """
        Get list of all unique metric names.

        Returns
        -------
        List[str]
            List of unique metric names

        """
        return list(set(dp.metric_name for dp in self.data_points))

    @property
    def metric_name(self) -> str:
        """
        Get the primary metric name.

        Returns the first metric name if there are multiple metrics,
        or empty string if no metrics.

        Returns
        -------
        str
            Primary metric name

        """
        names = self.metric_names
        return names[0] if names else ''


class MetricsManager(Manager):
    """
    SingleStoreDB metrics manager.

    This class should be instantiated using
    :func:`singlestoredb.manage_metrics` or accessed via
    :attr:`WorkspaceManager.metrics`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API

    """

    #: Object type
    obj_type = 'metrics'

    def get_workspace_group_metrics(
        self,
        organization_id: str,
        workspace_group_id: str,
    ) -> WorkspaceGroupMetrics:
        """
        Get metrics for a workspace group in OpenMetrics format.

        Parameters
        ----------
        organization_id : str
            ID of the organization
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        :class:`WorkspaceGroupMetrics`
            Parsed metrics data

        Examples
        --------
        >>> metrics_mgr = singlestoredb.manage_metrics()
        >>> metrics = metrics_mgr.get_workspace_group_metrics("org-123", "wg-456")
        >>> cpu_metrics = metrics.get_metrics_by_name(
        ...     "singlestoredb_cloud_cpu_usage"
        ... )
        >>> print(f"Found {len(cpu_metrics)} CPU data points")

        """
        url = (
            f'v2/organizations/{organization_id}/'
            f'workspaceGroups/{workspace_group_id}/metrics'
        )
        res = self._get(url)

        # The API returns text/plain OpenMetrics format
        metrics_text = res.text

        return WorkspaceGroupMetrics.from_openmetrics_text(
            workspace_group_id=workspace_group_id,
            metrics_text=metrics_text,
        )


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
        Version of the API to use
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
    >>> metrics = metrics_mgr.get_workspace_group_metrics("org-123", "wg-456")
    >>> print(f"Found {len(metrics.data_points)} metric data points")

    """
    return MetricsManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
