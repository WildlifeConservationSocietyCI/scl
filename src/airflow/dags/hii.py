import os
import pytz
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import state
from datetime import datetime, timedelta
import ee

ee.Initialize()
# Testing -- just create 1-2 runs
yesterday = datetime.now(pytz.utc) - timedelta(days=1)

default_args = {
    "owner": "root",
    "depends_on_past": False,
    "start_date": yesterday,
    "email": ["kfisher@wcs.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "rootdir": "users/kfisher/scl/v1",
    "rootdir": "users/kfisher/hii/v1",
    "env": os.environ.get('ENV') or 'dev',
}

hii = DAG("hii", default_args=default_args, schedule_interval=timedelta(1))


# BaseOperator(PythonOperator):
# env: local-kfisher, dev, prod, ... If not prod, appended to paths: os.path.join(rootdir, env)
# rootdir: path to ee root dir for holding assets
# inputs: [  <- Note this is a list there is a maxage for each input
#   {
#     "datastore": "ee",  # not sure about this - maybe should make an ee Connection or something
#     "path": "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG",
#     "task_id": "hii_power",
#     "maxage": {"years": 1},  # arg for https://dateutil.readthedocs.io/en/stable/relativedelta.html
#   },
# ]
# output: os.path.join(rootdir, env, "driver/power_influence"),
#
# HIIOperator(BaseOperator):
#
# SCLOperator(BaseOperator):
# species_aoi: path to multipoly
# aoi: [optional] path to multipoly to further limit calcs
#
# SCLOperator/HIIOperator instance init:
# make sure ee job callable will have everything it needs
# Note: sensors may have a role in the pseudocode below
# iterate over self.inputs for each:
# - find most recent asset prior to current task.execution_date
# - if now - that asset's timestamp is > self.inputs[i]['maxage'], mark task skipped and exit
# - else, get previous successful run of this task and look up the timestamp of its output.
# - track timestamp of asset used in last successful run of current task
# If all most-recent input timestamps are <= timestamp of last successful run of current task, mark task skipped and
# exit
# Otherwise, run ee job callable
# Deal with return val if successful, store timestamp of output ee asset, mark task successful and return

# callables should wrap/subclass an ee callable that handles ee initializing, connecting, waiting, handling all
# possible return vals


# example ee tasks

# poc_aoi = ee.FeatureCollection('users/kfisher/hf3/poc_aoi')
poc_aoi = ee.FeatureCollection('users/kfisher/scl_poc_aoi')


def calc_hii_power(ds, **kwargs):
    radiance_band = 'avg_rad'

    viirs = ee.Image(
        ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
        .sort('system:time_start', False)
        .select(radiance_band)
        .first()
    ).clip(poc_aoi)
    print(viirs)

    viirs_bins = viirs.reduceRegion(
        geometry=poc_aoi,
        reducer=ee.Reducer.percentile(
            percentiles=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            maxRaw=1000000
        ),
        scale=1000,
    )
    print(viirs_bins)

    avg_rad_p0 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p0'))).clip(poc_aoi)
    avg_rad_p10 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p10'))).clip(poc_aoi)
    avg_rad_p20 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p20'))).clip(poc_aoi)
    avg_rad_p30 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p30'))).clip(poc_aoi)
    avg_rad_p40 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p40'))).clip(poc_aoi)
    avg_rad_p50 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p50'))).clip(poc_aoi)
    avg_rad_p60 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p60'))).clip(poc_aoi)
    avg_rad_p70 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p70'))).clip(poc_aoi)
    avg_rad_p80 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p80'))).clip(poc_aoi)
    avg_rad_p90 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p90'))).clip(poc_aoi)
    avg_rad_p100 = ee.Image(ee.Number(viirs_bins.get('avg_rad_p100'))).clip(poc_aoi)
    power_influence = viirs.expression(
        " (viirs < avg_rad_p0) ? 0 \
        : (viirs < avg_rad_p10) ? 1 \
        : (viirs < avg_rad_p20) ? 2 \
        : (viirs < avg_rad_p30) ? 3 \
        : (viirs < avg_rad_p40) ? 4 \
        : (viirs < avg_rad_p50) ? 5 \
        : (viirs < avg_rad_p60) ? 6 \
        : (viirs < avg_rad_p70) ? 7 \
        : (viirs < avg_rad_p80) ? 8 \
        : (viirs < avg_rad_p90) ? 9 \
        : 10",
        {
            'viirs': viirs,
            'avg_rad_p0': avg_rad_p0,
            'avg_rad_p10': avg_rad_p10,
            'avg_rad_p20': avg_rad_p20,
            'avg_rad_p30': avg_rad_p30,
            'avg_rad_p40': avg_rad_p40,
            'avg_rad_p50': avg_rad_p50,
            'avg_rad_p60': avg_rad_p60,
            'avg_rad_p70': avg_rad_p70,
            'avg_rad_p80': avg_rad_p80,
            'avg_rad_p90': avg_rad_p90,
            'avg_rad_p100': avg_rad_p100,
        }
    ).clip(poc_aoi).rename('influence')

    power_export = ee.batch.Export.image.toAsset(
        image=power_influence,
        description='power_influence',
        assetId='users/kfisher/HII/v1/dev/driver/power_influence',
        region=poc_aoi.geometry().getInfo()['coordinates'],
        scale=1000,
    )
    power_export.start()
    time.sleep(1)
    print(power_export.id, power_export.state)

    return 'ran calc_hii_power'


def calc_hii_roads(ds, **kwargs):
    direct_influence = 8
    direct_influence_distance = 500  # m
    indirect_influence = 4
    max_influence_distance = 15000
    decay_constant = -0.0002  # lose 18 % / 1000 m
    tiger = ee.FeatureCollection("users/aduncan/osm_global/highway-primary")

    roads = tiger.filterBounds(poc_aoi.geometry())
    road_distance = roads.distance(max_influence_distance).clip(poc_aoi.geometry())
    road_distance0 = road_distance.unmask(max_influence_distance)

    roads_direct_influence = ee.Image(0).where(
        road_distance0.lt(direct_influence_distance),
        ee.Image(direct_influence)
    )
    roads_indirect_influence_area = ee.Image(0).where(
        road_distance0.gte(direct_influence_distance).And(road_distance0.lt(max_influence_distance)),
        road_distance0
    )
    roads_indirect_influence = roads_indirect_influence_area.updateMask(roads_indirect_influence_area) \
        .subtract(ee.Image(direct_influence_distance)) \
        .multiply(decay_constant).exp() \
        .multiply(indirect_influence)
    
    roads_influence = roads_direct_influence.add(roads_indirect_influence.unmask()) \
        .updateMask(road_distance.lt(max_influence_distance)) \
        .clip(poc_aoi.geometry()) \
        .rename('influence')

    roads_export = ee.batch.Export.image.toAsset(
        image=roads_influence, 
        description='road_influence', 
        assetId='users/kfisher/HII/v1/dev/driver/road_influence',
        region=poc_aoi.geometry().getInfo()['coordinates'],
        scale=1000,
    )
    roads_export.start()
    time.sleep(1)
    print(roads_export.id, roads_export.state)

    return 'ran calc_hii_roads'


def calc_hii(ds, **kwargs):
    road_influence = ee.Image("users/kfisher/hf3/output/drivers/road_influence").float()
    power_influence = ee.Image("users/kfisher/hf3/output/drivers/power_influence").float()

    hii_sum = ee.ImageCollection(ee.List([road_influence, power_influence])).sum()

    hii_nonulls = hii_sum \
        .updateMask(power_influence.remap(ee.List([0]), ee.List([1]), 1)) \
        .updateMask(road_influence.remap(ee.List([0]), ee.List([1]), 1))

    hii_export = ee.batch.Export.image.toAsset(
        image=hii_nonulls,
        description='hii',
        assetId='users/kfisher/HII/v1/dev/hii',
        region=poc_aoi.geometry().getInfo()['coordinates'],
        scale=1000,
    )
    hii_export.start()
    time.sleep(1)
    print(hii_export.id, hii_export.state)

    return 'ran calc_hii'


def check_inputs_ready(context):
    # ee.batch.Task.list()
    time.sleep(60 * 5)
    return True


class EECompletionSensor(BaseSensorOperator):

    def __init__(self, *args, **kwargs):
        super(EECompletionSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        print('poking {}'.__str__())

        # poke functions should return a boolean
        return check_inputs_ready(context)


hii_power_task = PythonOperator(
    task_id='hii_power',
    provide_context=True,
    python_callable=calc_hii_power,
    dag=hii,
)

hii_roads_task = PythonOperator(
    task_id='hii_roads',
    provide_context=True,
    python_callable=calc_hii_roads,
    dag=hii,
)

hii_task_inputs = EECompletionSensor(
    task_id='hii_task_inputs',
    dag=hii,
)

hii_task = PythonOperator(
    task_id='hii_add_drivers',
    provide_context=True,
    python_callable=calc_hii,
    dag=hii,
)

hii_task_inputs.set_upstream(hii_power_task)
hii_task_inputs.set_upstream(hii_roads_task)
hii_task.set_upstream(hii_task_inputs)
hii_task.set_upstream(hii_task_inputs)
