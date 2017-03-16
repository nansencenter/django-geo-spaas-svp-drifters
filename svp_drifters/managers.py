import numpy as np
import datetime
from dateutil.parser import parse

import pythesint as pti

from django.db import models
from django.contrib.gis.geos import LineString

from geospaas.utils import validate_uri, nansat_filename

from geospaas.vocabularies.models import Platform
from geospaas.vocabularies.models import Instrument
from geospaas.vocabularies.models import DataCenter
from geospaas.vocabularies.models import ISOTopicCategory
from geospaas.catalog.models import GeographicLocation
from geospaas.catalog.models import DatasetURI, Source, Dataset

# Demo filename: /vagrant/shared/test_data/drifters/buoydata_15001_sep16.dat
class SVPDrifterManager(models.Manager):
    def add_svp_drifter_data_from_file(self, uri, period_days=1):
        ''' Create all datasets from given file and add corresponding metadata

        Parameters:
        ----------
            uri : str
                  URI to file
        Returns:
        -------
            dataset and flag
        '''
        # set metadata
        pp = Platform.objects.get(short_name='BUOYS')
        ii = Instrument.objects.get(short_name = 'DRIFTING BUOYS')
        src = Source.objects.get_or_create(platform = pp, instrument = ii)[0]
        dc = DataCenter.objects.get(short_name = 'DOC/NOAA/OAR/AOML')
        iso = ISOTopicCategory.objects.get(name='Oceans')

        # read dates, lon, lat from file
        dates, lon, lat = get_dates_lon_lat(uri)
        dates0 = parse(dates[0])
        dates1 = parse(dates[-1])

        # add all 1-day chunks to database
        date = datetime.datetime(dates0.year, dates0.month, dates0.day)
        cnt = 0
        while date < dates1:
            start_date = date
            end_date = start_date + datetime.timedelta(period_days)
            lon, lat = get_lon_lat(uri, start_date, end_date)
            self.add_trajectory(uri, start_date, end_date, lon, lat, src, dc, iso)
            date = end_date
            cnt += 1

        return cnt

    def add_trajectory(self, uri, start_date, end_date, lon, lat, src, dc, iso):
        ''' Add one chunk of trajectory to database '''
        line1 = LineString((zip(lon, lat)))
        geolocation = GeographicLocation.objects.get_or_create(geometry=line1)[0]

        ds = Dataset.objects.get_or_create(
                    entry_title=uri + start_date.strftime('_%Y-%m-%d'),
                    ISO_topic_category = iso,
                    data_center = dc,
                    summary = uri,
                    time_coverage_start=start_date,
                    time_coverage_end=end_date,
                    source=src,
                    geographic_location=geolocation)[0]

        ds_uri = DatasetURI.objects.get_or_create(uri=uri, dataset=ds)[0]

