import minimal_honeycomb
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

def fetch_ble_radio_pings(
    start=None,
    end=None,
    environment_id=None,
    environment_name=None,
    return_tag_name=False,
    return_tag_tag_id=False,
    return_anchor_name=False,
    tag_device_types=['BLETAG'],
    anchor_device_types=['PIZERO', 'PI3', 'PI3WITHCAMERA'],
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Resolving environment specification')
    environment_id = fetch_environment_id(
        environment_id=environment_id,
        environment_name=environment_name,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    if environment_id is None:
        raise ValueError('Must specify environment')
    logger.info('Fetching tag device assignments')
    tag_assignments_df = fetch_device_assignments(
        environment_id=environment_id,
        start=start,
        end=end,
        device_types=tag_device_types,
        column_name_prefix='tag',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('{} tag device assignments matched specified criteria'.format(len(tag_assignments_df)))
    logger.info('Fetching anchor device assignments')
    anchor_assignments_df = fetch_device_assignments(
        environment_id=environment_id,
        start=start,
        end=end,
        device_types=anchor_device_types,
        column_name_prefix='anchor',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('{} anchor device assignments matched specified criteria'.format(len(anchor_assignments_df)))
    tag_device_ids = tag_assignments_df['tag_device_id'].unique().tolist()
    anchor_device_ids = anchor_assignments_df['anchor_device_id'].unique().tolist()
    logger.info('Building query list for BLE datapoint search')
    query_list = list()
    query_list.append({
        'field': 'tag_device',
        'operator': 'IN',
        'values': tag_device_ids
    })
    query_list.append({
        'field': 'anchor_device',
        'operator': 'IN',
        'values': anchor_device_ids
    })
    if start is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'GTE',
            'value': minimal_honeycomb.to_honeycomb_datetime(start)
        })
    if end is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'LTE',
            'value': minimal_honeycomb.to_honeycomb_datetime(end)
        })
    return_data= [
        'radio_ping_id',
        'timestamp',
        {'tag_device': [
            'device_id'
        ]},
        {'anchor_device': [
            'device_id'
        ]},
        'signal_strength'
    ]
    result = search_ble_radio_pings(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    data = list()
    logger.info('Parsing {} returned radio_pings'.format(len(result)))
    for datum in result:
        data.append({
            'radio_ping_id': datum.get('radio_ping_id'),
            'timestamp': datum.get('timestamp'),
            'tag_device_id': datum.get('tag_device').get('device_id'),
            'anchor_device_id': datum.get('anchor_device').get('device_id'),
            'rssi': float(datum.get('signal_strength'))
        })
    radio_pings_df = pd.DataFrame(data)
    radio_pings_df['timestamp'] = pd.to_datetime(radio_pings_df['timestamp'])
    radio_pings_df.set_index('radio_ping_id', inplace=True)
    radio_pings_df = radio_pings_df.join(tag_assignments_df.set_index('tag_device_id'), on='tag_device_id')
    radio_pings_df = radio_pings_df.join(anchor_assignments_df.set_index('anchor_device_id'), on='anchor_device_id')
    return_columns = [
        'timestamp',
        'tag_device_id',
    ]
    if return_tag_name:
        return_columns.append('tag_name')
    if return_tag_tag_id:
        return_columns.append('tag_tag_id')
    return_columns.extend([
        'anchor_device_id'
    ])
    if return_anchor_name:
        return_columns.append('anchor_name')
    return_columns.append('rssi')
    radio_pings_df = radio_pings_df.reindex(columns=return_columns)
    return radio_pings_df

def fetch_ble_datapoints(
    start=None,
    end=None,
    environment_id=None,
    environment_name=None,
    return_tag_name=False,
    return_tag_tag_id=False,
    return_anchor_name=False,
    tag_device_types=['BLETAG'],
    anchor_device_types=['PIZERO', 'PI3', 'PI3WITHCAMERA'],
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Resolving environment specification')
    environment_id = fetch_environment_id(
        environment_id=environment_id,
        environment_name=environment_name,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    if environment_id is None:
        raise ValueError('Must specify environment')
    logger.info('Fetching tag device assignments')
    tag_assignments_df = fetch_device_assignments(
        environment_id=environment_id,
        start=start,
        end=end,
        device_types=tag_device_types,
        column_name_prefix='tag',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('{} tag device assignments matched specified criteria'.format(len(tag_assignments_df)))
    logger.info('Fetching anchor device assignments')
    anchor_assignments_df = fetch_device_assignments(
        environment_id=environment_id,
        start=start,
        end=end,
        device_types=anchor_device_types,
        column_name_prefix='anchor',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('{} anchor device assignments matched specified criteria'.format(len(anchor_assignments_df)))
    tag_assignment_ids = tag_assignments_df.index.unique().tolist()
    logger.info('Building query list for BLE datapoint search')
    query_list = list()
    query_list.append({
        'field': 'source',
        'operator': 'IN',
        'values': tag_assignment_ids
    })
    if start is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'GTE',
            'value': minimal_honeycomb.to_honeycomb_datetime(start)
        })
    if end is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'LTE',
            'value': minimal_honeycomb.to_honeycomb_datetime(end)
        })
    return_data= [
        'data_id',
        'timestamp',
        {'source': [
            {'... on Assignment': [
                'assignment_id'
            ]}
        ]},
        {'file': [
            'data'
        ]}
    ]
    result = search_ble_datapoints(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    data = list()
    logger.info('Parsing {} returned datapoints'.format(len(result)))
    for datum in result:
        data_dict = json.loads(datum.get('file').get('data'))
        data.append({
            'data_id': datum.get('data_id'),
            'timestamp': datum.get('timestamp'),
            'tag_assignment_id': datum.get('source').get('assignment_id'),
            'anchor_id': data_dict.get('anchor_id'),
            'rssi': float(data_dict.get('rssi'))
        })
    datapoints_df = pd.DataFrame(data)
    datapoints_df['timestamp'] = pd.to_datetime(datapoints_df['timestamp'])
    datapoints_df.set_index('data_id', inplace=True)
    datapoints_df = datapoints_df.join(tag_assignments_df, on='tag_assignment_id')
    assignment_anchor_ids_df = anchor_assignments_df.copy().reset_index()
    assignment_anchor_ids_df['anchor_id'] = assignment_anchor_ids_df['anchor_assignment_id']
    assignment_anchor_ids_df.set_index('anchor_id', inplace=True)
    device_anchor_ids_df = anchor_assignments_df.copy().reset_index()
    device_anchor_ids_df['anchor_id'] = device_anchor_ids_df['anchor_device_id']
    device_anchor_ids_df.set_index('anchor_id', inplace=True)
    anchor_ids_df = pd.concat((assignment_anchor_ids_df, device_anchor_ids_df))
    datapoints_df = datapoints_df.join(anchor_ids_df, on='anchor_id')
    return_columns = [
        'timestamp',
        'tag_assignment_id',
        'tag_device_id',
    ]
    if return_tag_name:
        return_columns.append('tag_name')
    if return_tag_tag_id:
        return_columns.append('tag_tag_id')
    return_columns.extend([
        'anchor_assignment_id',
        'anchor_device_id'
    ])
    if return_anchor_name:
        return_columns.append('anchor_name')
    return_columns.append('rssi')
    datapoints_df = datapoints_df.reindex(columns=return_columns)
    return datapoints_df

def write_ble_radio_pings(
    datapoints_df,
    source_id_column_name='tag_assignment_id',
    source_type='MEASURED',
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    datapoints_df = datapoints_df.rename(
        columns={
            'tag_device_id': 'tag_device',
            'anchor_device_id': 'anchor_device',
            'rssi': 'signal_strength',
            source_id_column_name: 'source'
        }
    )
    datapoints_df = datapoints_df.reindex(columns=[
        'timestamp',
        'tag_device',
        'anchor_device',
        'signal_strength',
        'source'
    ])
    datapoints_df['timestamp'] = datapoints_df['timestamp'].apply(lambda x:
        minimal_honeycomb.to_honeycomb_datetime(x.to_pydatetime())
    )
    datapoints_df['source_type'] = source_type
    datapoints_list = datapoints_df.to_dict(orient='records')
    logger.info('Writing data for {} radio pings to Honeycomb'.format(len(datapoints_list)))
    if client is None:
        client = minimal_honeycomb.MinimalHoneycombClient(
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    result = client.bulk_mutation(
        request_name='createRadioPing',
        arguments={
            'radioPing': {
                'type': 'RadioPingInput',
                'value': datapoints_list
            }
        },
        return_object=[
            'radio_ping_id'
        ],
        chunk_size=chunk_size
    )
    radio_ping_ids = [datum.get('radio_ping_id') for datum in result]
    return radio_ping_ids

def fetch_environment_id(
    environment_id=None,
    environment_name=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if environment_id is not None:
        if environment_name is not None:
            raise ValueError('If environment ID is specified, environment name cannot be specified')
        return environment_id
    if environment_name is not None:
        logger.info('Fetching environment ID for specified environment name')
        if client is None:
            client = minimal_honeycomb.MinimalHoneycombClient(
                uri=uri,
                token_uri=token_uri,
                audience=audience,
                client_id=client_id,
                client_secret=client_secret
            )
        result = client.bulk_query(
            request_name='findEnvironments',
            arguments={
                'name': {
                    'type': 'String',
                    'value': environment_name
                }
            },
            return_data=[
                'environment_id'
            ],
            id_field_name='environment_id'
        )
        if len(result) == 0:
            raise ValueError('No environments match environment name {}'.format(
                environment_name
            ))
        if len(result) > 1:
            raise ValueError('Multiple environments match environment name {}'.format(
                environment_name
            ))
        environment_id = result[0].get('environment_id')
        logger.info('Found environment ID for specified environment name')
        return environment_id
    return None

def fetch_device_info(
    device_ids,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Building query list for device search')
    query_list=list()
    query_list.append({
        'field': 'device_id',
        'operator': 'IN',
        'values': device_ids
    })
    return_data= [
        'device_id',
        'part_number',
        'device_type',
        'name',
        'tag_id',
        'serial_number'
    ]
    result = search_devices(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('{} devices found for specified device IDs'.format(len(result)))
    data = list()
    for datum in result:
        data.append({
            'device_id': datum.get('device_id'),
            'device_part_number': datum.get('part_number'),
            'device_type': datum.get('device_type'),
            'device_name': datum.get('name'),
            'device_tag_id': datum.get('tag_id'),
            'device_serial_number': datum.get('serial_number'),
        })
    devices_df = pd.DataFrame(data)
    devices_df.set_index('device_id', inplace=True)
    return_columns = [
        'device_part_number',
        'device_type',
        'device_name',
        'device_tag_id',
        'device_serial_number',
    ]
    devices_df = devices_df.reindex(columns=return_columns)
    return devices_df

def fetch_device_assignments(
    environment_id,
    start=None,
    end=None,
    device_types=None,
    column_name_prefix=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Building query list for device assignment search')
    query_list=list()
    query_list.append({
        'field': 'environment',
        'operator': 'EQ',
        'value': environment_id
    })
    query_list.append({
        'field': 'assigned_type',
        'operator': 'EQ',
        'value': 'DEVICE'
    })
    return_data= [
        'assignment_id',
        'start',
        'end',
        {'assigned': [
            {'... on Device': [
                'device_id',
                'part_number',
                'device_type',
                'name',
                'tag_id',
                'serial_number'
            ]}
        ]}
    ]
    result = search_assignments(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('{} device assignments found for specified environment'.format(len(result)))
    filtered_result = minimal_honeycomb.filter_assignments(
        assignments=result,
        start_time=start,
        end_time=end
    )
    logger.info('{} device assignments are consistent with the specified time window'.format(len(filtered_result)))
    data = list()
    for datum in filtered_result:
        if device_types is None or datum.get('assigned').get('device_type') in device_types:
            data.append({
                'assignment_id': datum.get('assignment_id'),
                'device_id': datum.get('assigned').get('device_id'),
                'device_type': datum.get('assigned').get('device_type'),
                'name': datum.get('assigned').get('name'),
                'part_number': datum.get('assigned').get('part_number'),
                'serial_number': datum.get('assigned').get('serial_number'),
                'tag_id': datum.get('assigned').get('tag_id'),
            })
    logger.info('{} device assignments are consistent with the specified device types'.format(len(data)))
    assignments_df = pd.DataFrame(data)
    assignments_df.set_index('assignment_id', inplace=True)
    return_columns = [
        'device_id',
        'device_type',
        'name',
        'part_number',
        'serial_number',
        'tag_id'
    ]
    assignments_df = assignments_df.reindex(columns=return_columns)
    if column_name_prefix is not None:
        assignments_df.index.name = column_name_prefix + '_' + assignments_df.index.name
        assignments_df.columns = [column_name_prefix + '_' + column_name for column_name in assignments_df.columns]
    return assignments_df

def fetch_device_person_assignments(
    device_ids,
    start=None,
    end=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Building query list for entity assignment search')
    query_list=list()
    query_list.append({
        'field': 'device',
        'operator': 'IN',
        'values': device_ids
    })
    return_data= [
        'entity_assignment_id',
        'start',
        'end',
        'entity_type',
        {'entity': [
            {'...on Person': [
                'person_id',
                'name',
                'first_name',
                'last_name',
                'nickname',
                'short_name',
                'person_type',
                'transparent_classroom_id'
            ]}
        ]},
        {'device': [
            'device_id'
        ]}
    ]
    result = search_entity_assignments(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('{} entity assignments found for specified devices'.format(len(result)))
    filtered_result = minimal_honeycomb.filter_assignments(
        assignments=result,
        start_time=start,
        end_time=end
    )
    logger.info('{} entity assignments are consistent with the specified time window'.format(len(filtered_result)))
    data = list()
    for datum in filtered_result:
        if datum.get('entity_type') == 'PERSON':
            data.append({
                'entity_assignment_id': datum.get('entity_assignment_id'),
                'device_id': datum.get('device').get('device_id'),
                'person_id': datum.get('entity').get('person_id'),
                'person_name': datum.get('entity').get('name'),
                'person_first_name': datum.get('entity').get('first_name'),
                'person_last_name': datum.get('entity').get('last_name'),
                'person_nickname': datum.get('entity').get('nickname'),
                'person_short_name': datum.get('entity').get('short_name'),
                'person_type': datum.get('entity').get('person_type'),
                'person_transparent_classroom_id': datum.get('entity').get('transparent_classroom_id')
            })
    logger.info('{} entity assignments correspond to people'.format(len(data)))
    person_assignments_df = pd.DataFrame(data)
    if sum(person_assignments_df['device_id'].duplicated()) > 0:
        raise ValueError('Some specified devices have multiple person assigments in the specified time window')
    person_assignments_df.set_index('device_id', inplace=True)
    return_columns = [
        'person_id',
        'person_name',
        'person_first_name',
        'person_last_name',
        'person_nickname',
        'person_short_name',
        'person_type',
        'person_transparent_classroom_id'
    ]
    person_assignments_df = person_assignments_df.reindex(columns=return_columns)
    return person_assignments_df

def fetch_device_position_assignments(
    device_ids,
    coordinate_space_id,
    start=None,
    end=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Building query list for position assignment search')
    query_list=list()
    query_list.append({
        'field': 'assigned',
        'operator': 'IN',
        'values': device_ids
    })
    query_list.append({
        'field': 'coordinate_space',
        'operator': 'EQ',
        'value': coordinate_space_id
    })
    return_data= [
        'position_assignment_id',
        'start',
        'end',
        {'assigned': [
            {'... on Device': [
                'device_id'
            ]}
        ]},
        'coordinates'
    ]
    result = search_position_assignments(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('{} position assignments found for specified devices'.format(len(result)))
    filtered_result = minimal_honeycomb.filter_assignments(
        assignments=result,
        start_time=start,
        end_time=end
    )
    logger.info('{} position assignments are consistent with the specified time window'.format(len(filtered_result)))
    data = list()
    for datum in filtered_result:
        data.append({
            'position_assignment_id': datum.get('position_assignment_id'),
            'device_id': datum.get('assigned').get('device_id'),
            'x_meters': float(datum.get('coordinates')[0]),
            'y_meters': float(datum.get('coordinates')[1]),
            'z_meters': float(datum.get('coordinates')[2])
        })
    position_assignments_df = pd.DataFrame(data)
    if sum(position_assignments_df['device_id'].duplicated()) > 0:
        raise ValueError('Some specified devices have multiple position assigments in the specified time window')
    position_assignments_df.set_index('device_id', inplace=True)
    return_columns = [
        'x_meters',
        'y_meters',
        'z_meters'
    ]
    position_assignments_df = position_assignments_df.reindex(columns=return_columns)
    return position_assignments_df

def search_ble_radio_pings(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for BLE radio pings that match the specified parameters')
    result = search_objects(
        request_name='searchRadioPings',
        query_list=query_list,
        return_data=return_data,
        id_field_name='radio_ping_id',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('Returned {} radio pings'.format(len(result)))
    return result

def search_ble_datapoints(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for BLE datapoints that match the specified parameters')
    result = search_objects(
        request_name='searchDatapoints',
        query_list=query_list,
        return_data=return_data,
        id_field_name='data_id',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('Returned {} datapoints'.format(len(result)))
    return result

def search_devices(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for devices that match the specified parameters')
    result = search_objects(
        request_name='searchDevices',
        query_list=query_list,
        return_data=return_data,
        id_field_name='device_id',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('Returned {} devices'.format(len(result)))
    return result

def search_assignments(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for assignments that match the specified parameters')
    result = search_objects(
        request_name='searchAssignments',
        query_list=query_list,
        return_data=return_data,
        id_field_name='assignment_id',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('Returned {} assignments'.format(len(result)))
    return result

def search_entity_assignments(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for entity assignments that match the specified parameters')
    result = search_objects(
        request_name='searchEntityAssignments',
        query_list=query_list,
        return_data=return_data,
        id_field_name='entity_assignment_id',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('Returned {} entity assignments'.format(len(result)))
    return result

def search_position_assignments(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for position assignments that match the specified parameters')
    result = search_objects(
        request_name='searchPositionAssignments',
        query_list=query_list,
        return_data=return_data,
        id_field_name='position_assignment_id',
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
    )
    logger.info('Returned {} position assignments'.format(len(result)))
    return result

def search_objects(
    request_name,
    query_list,
    return_data,
    id_field_name,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if client is None:
        client = minimal_honeycomb.MinimalHoneycombClient(
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    result = client.bulk_query(
        request_name=request_name,
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'operator': 'AND',
                    'children': query_list
                }
            }
        },
        return_data=return_data,
        id_field_name=id_field_name,
        chunk_size=chunk_size
    )
    return result
