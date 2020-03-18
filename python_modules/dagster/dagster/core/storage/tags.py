from dagster import check

SYSTEM_TAG_PREFIX = 'dagster/'

SCHEDULE_NAME_TAG = '{prefix}schedule_name'.format(prefix=SYSTEM_TAG_PREFIX)

BACKFILL_ID_TAG = '{prefix}backfill'.format(prefix=SYSTEM_TAG_PREFIX)

PARTITION_NAME_TAG = '{prefix}partition'.format(prefix=SYSTEM_TAG_PREFIX)

PARTITION_SET_TAG = '{prefix}partition_set'.format(prefix=SYSTEM_TAG_PREFIX)


def check_tags(obj, name):
    check.opt_dict_param(obj, name, key_type=str, value_type=str)

    for tag in obj.keys():
        check.invariant(
            not tag.startswith(SYSTEM_TAG_PREFIX),
            desc='User attempted to set tag with reserved system prefix: {tag}'.format(tag=tag),
        )
