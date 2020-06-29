import minimal_honeycomb
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

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
