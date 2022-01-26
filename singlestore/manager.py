
import requests
from collections.abc import Sequence
from typing import Union, Optional
from urllib.parse import urljoin
from . import exceptions


class Region(object):

    def __init__(self, region_id: str, region: str, provider: str) -> 'Region':
        self.id = region_id
        self.region = region
        self.provider = provider
        self._manager = None

    @classmethod
    def from_dict(cls, obj, manager: Optional['ClusterManager']=None) -> 'Region':
        out = cls(region_id=obj['regionId'],
                  region=obj['region'],
                  provider=obj['provider'])
        out._manager = manager
        return out


class Cluster(object):

    def __init__(self, name: str, cluster_id: str, region: Region, size: str,
                 units: float, state: str, version: str,
                 created_at: str, expires_at: Optional[str]=None,
                 firewall_ranges: Optional[Sequence[str]]=None) -> 'Cluster':
        self.name = name
        self.id = cluster_id
        self.region = region
        self.size = size
        self.units = units
        self.state = state
        self.version = version
        self.created_at = created_at
        self.expires_at = expires_at
        self.firewall_ranges = firewall_ranges
        self._manager = None

    @classmethod
    def from_dict(cls, obj, manager: Optional['ClusterManager']=None) -> 'Cluster':
        out = cls(name=obj['name'], cluster_id=obj['clusterId'],
                  region=Region.from_dict(obj['region'], manager=manager),
                  size=obj['size'], units=obj['units'],
                  state=obj['state'], version=obj['version'],
                  created_at=obj['createdAt'], expires_at=obj['expiresAt'],
                  firewall_ranges=obj['firewallRanges'],
                  terminated_at=obj['terminatedAt'])
        out._manager = manager
        return out

    def update(self, name: Optional[str]=None,
            admin_password: Optional[str]=None,
            expires_at: Optional[str]=None,
            size: Optional[str]=None, firewall_ranges: Sequence[str]=None):
        data = {k: v for k, v in dict(name=name, adminPassword=admin_password,
                                      expiresAt=expires_at, size=size,
                                      firewallRanges=firewall_ranges) if v is not None}
        self._manager._patch(f'clusters/{self.cluster_id}', json=data)

    def suspend(self):
        self._manager._post(f'clusters/{self.cluster_id}/suspend',
                            headers={'Content-Type': 'application/x-www-form-urlencoded'})

    def resume(self):
        self._manager._post(f'clusters/{self.cluster_id}/resume',
                            headers={'Content-Type': 'application/x-www-form-urlencoded'})

    def terminate(self):
        self._manager._delete(f'clusters/{self.cluster_id}')


class ClusterManager(object):

    def __init__(self, base_url: str='https://api.singlestore.com',
                 access_token: str=None, version: str='v0beta') -> 'ClusterManager':
        access_token = access_token or os.environ.get('SINGLESTORE_MANAGEMENT_TOKEN', None)
        self._sess = requests.Session()
        self._sess.headers.update({
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        self._base_url = urljoin(base_url, version) + '/'

    def _check(self, res):
        if res.status_code >= 400:
            raise exceptions.ClusterError(res.status_code, res.text)
        return res

    def _get(self, path, *args, **kwargs):
        return self._check(self._sess.get(urljoin(self._base_url, path), *args, **kwargs))

    def _post(self, path, *args, **kwargs):
        return self._check(self._sess.post(urljoin(self._base_url, path), *args, **kwargs))

    def _put(self, path, *args, **kwargs):
        return self._check(self._sess.put(urljoin(self._base_url, path), *args, **kwargs))

    def _delete(self, path, *args, **kwargs):
        return self._check(self._sess.delete(urljoin(self._base_url, path), *args, **kwargs))

    def _patch(self, path, *args, **kwargs):
        return self._check(self._sess.patch(urljoin(self._base_url, path), *args, **kwargs))

    @property
    def clusters(self) -> Sequence[Cluster]:
        res = self._get('clusters')
        return [Cluster.from_dict(item, manager=self) for item in res.json()]

    @property
    def regions(self) -> Sequence[Region]:
        res = self._get('regions')
        return [Region.from_dict(item, manager=self) for item in res.json()]

    def create_cluster(self, name: str, region_id: str, admin_password: str,
            firewall_ranges: Sequence[str], expires_at: Optional[str]=None,
            size: Optional[str]=None, plan: Optional[str]=None) -> Cluster:
        res = self._post('clusters', json=dict(
            name=name, regionID=region_id, adminPassword=admin_password,
            expiresAt=expires_at, size=size, firewallRanges=firewall_ranges,
            plan=plan
        ))
        return Cluster.from_obj(res.json(), manager=self)

    def get_cluster(self, cluster_id: str):
        res = self._get(f'clusters/{cluster_id}')
        return Cluster.from_obj(res.json(), manager=self)


def manage(base_url: str='https://api.singlestore.com',
           access_token: str=None, version: str='v0beta') -> ClusterManager:
    return ClusterManager(base_url=base_url, access_token=access_token, version=version)
