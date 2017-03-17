import re
import numpy as np
import datetime
from dateutil.parser import parse

import pythesint as pti

from django.db import models
from django.utils import timezone
from django.contrib.gis.geos import LineString

from geospaas.utils import validate_uri, nansat_filename

from geospaas.vocabularies.models import Platform
from geospaas.vocabularies.models import Instrument
from geospaas.vocabularies.models import DataCenter
from geospaas.vocabularies.models import ISOTopicCategory
from geospaas.catalog.models import GeographicLocation
from geospaas.catalog.models import DatasetURI, Source, Dataset

# Demo uri: file://localhost/vagrant/shared/test_data/drifters/buoydata_15001_sep16.dat
class SVPDrifterManager(models.Manager):

    def add_svp_drifters(self, uri_metadata, uri_data, maxnum=0):
        ''' Create all datasets from given file and add corresponding metadata

        Parameters:
        ----------
            uri_data : str
                  URI to file
            uri_metadata : str
                  URI to metadata file
        Returns:
        -------
            count : Number of ingested buoy datasets
        '''
        # set metadata
        pp = Platform.objects.get(short_name='BUOYS')
        ii = Instrument.objects.get(short_name = 'DRIFTING BUOYS')
        src = Source.objects.get_or_create(platform = pp, instrument = ii)[0]
        dc = DataCenter.objects.get(short_name = 'DOC/NOAA/OAR/AOML')
        iso = ISOTopicCategory.objects.get(name='Oceans')

        #'ID WMC_id experimentNumber BuoyType deploymentDate deploymetTime deplat deplon endDate endTime endlat endlon drogueLostDate drogueLostTime deathReason'
        #'11846540 4400770  2222    SVPB  2012/07/17 10:00   59.61   317.61 2015/11/29 15:00   57.66   352.24 2012/11/11 04:04  1\n'
        # Death reasons: 0=buoy still alive, 1=buoy ran aground, 2=picked up by
        # vessel, 3=stop transmitting, 4=sporadic transmissions, 5=bad
        # batteries, 6=inactive status
        # Get and loop drifter identification numbers
        count = 0
        with open(nansat_filename(uri_metadata)) as ff:
            for line in ff:
                m = re.search('^\s*(\d+)\s+\d+\s+\d+\s+(\w+)\s+(\d{4}/\d{2}/\d{2})\s+(\d{2}:\d{2})\s+\-?\d+\.\d+\s+\-?\d+\.\d+\s+(\d{4}/\d{2}/\d{2})\s+(\d{2}:\d{2})\s+.*\n$',line)
                id = int(m.group(1))
                buoyType = m.group(2)
                deploymentDate = m.group(3)
                deploymentTime = m.group(4)
                endDate = m.group(5)
                endTime = m.group(6)
                # Add drifter trajectory and metadata to database
                ds, created = Dataset.objects.get_or_create(
                    entry_title = '%s drifter no. %d'%(buoyType, id),
                    ISO_topic_category = iso,
                    data_center = dc,
                    summary = '',
                    time_coverage_start = \
                            timezone.datetime.strptime(deploymentDate +
                                deploymentTime, '%Y/%m/%d%H:%M').replace(
                                    tzinfo=timezone.utc),
                    time_coverage_end = \
                            timezone.datetime.strptime(endDate + endTime,
                                '%Y/%m/%d%H:%M').replace(tzinfo=timezone.utc),
                    source=src)
                meta_uri, muc = DatasetURI.objects.get_or_create(uri=uri_metadata, dataset=ds)
                data_uri, duc = DatasetURI.objects.get_or_create(uri=uri_data, dataset=ds)
                if created:
                    ds.geographic_location=self.trajectory(id, nansat_filename(uri_data))
                    ds.save()
                    count += 1
                    if maxnum and count>=maxnum:
                        break
            ff.close()
        return count

    def trajectory(self, id, datfile):
        ''' Add trajectory to database

        Read id, month, day, year, latitude, longitude, temperature, zonal velocity, meridional velocity, speed")
        '''
        lonlat = []
        with open(datfile) as ff:
            for line in ff:
                if '%d'%id in line:
                    m = re.search('^\s*(\d+)\s+\d+\s+\d+\.?\d+?\s+\d+\s+(\-?\d+\.\d+)\s+(\-?\d+\.\d+)\s+.*\n$',line)
                    if m and int(m.group(1))==id:
                        lon = float(m.group(3))
                        lat = float(m.group(2))
                        lonlat.append((lon, lat))
            ff.close()
        line1 = LineString((lonlat))
        geolocation = GeographicLocation.objects.get_or_create(geometry=line1)[0]

        return geolocation

