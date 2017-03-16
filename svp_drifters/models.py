from __future__ import unicode_literals

from django.db import models

from geospaas.catalog.models import Dataset as CatalogDataset
from geospaas.svp_drifters.managers import SVPDrifterManager

class LanceBuoy(CatalogDataset):
    class Meta:
        proxy = True
    objects = SVPDrifterManager()
