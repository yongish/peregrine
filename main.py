import argparse
import json
import pprint
from typing import Dict, List, Tuple

Aggregation = Tuple[str, int]

def filter_union(data, req_property_key, req_property_value_list):
    filtered_data = []
    for object in data:
        # c. Multiple property values should union.
        for property_dict in object['properties']:
            if property_dict['slug'] == req_property_key:
                compare_list = []
                if property_dict['type'] == 'string':
                    compare_list = [str(value) for value in req_property_value_list]
                elif property_dict['type'] == 'integer':
                    compare_list = [int(value) for value in req_property_value_list]
                else:   # boolean
                    compare_list = [bool(value) for value in req_property_value_list]

                if property_dict['value'] in compare_list:
                    filtered_data.append(object)
                    continue
    return filtered_data


def run(data: List[dict], models: List[str], properties: List[str]) -> Dict[str, List[Aggregation]]:
    """
    Takes a list of entity objects, filters data matching the `models` and `properties` specifications,
    and then aggregates the data returning a sorted list of aggregations.

    :param data: The list entity data
    :param models: A list of models to filter the aggregation on
    :param properties: A list of property keys and values to filter the aggregation on. Format:
        key:value1,value2
    """

    req_value_freq_dict = {}
    if len(properties) == 0:
        # a. Multiple models should union.
        filtered_data = [object for object in data if object['model'] in models]
        # If there are no properties, aggregate on everything.
        for object in filtered_data:
            for property_dict in object['properties']:
                req_value_freq_dict.setdefault(property_dict['slug'], {}).setdefault(property_dict['value'], 0)
                req_value_freq_dict[property_dict['slug']][property_dict['value']] += 1
    elif len(models) == 0:
        # Note: I added this logic at the last minute and am unsure if it's correct.
        for object in data:
            for property_dict in object['properties']:
                req_value_freq_dict.setdefault(property_dict['slug'], {}).setdefault(property_dict['value'], 0)
                req_value_freq_dict[property_dict['slug']][property_dict['value']] += 1
    else:
        agg_property_dict = {}
        for raw_property in properties:
            split_property = raw_property.split(':')
            value_list = split_property[1].split(',')
            agg_property_dict.setdefault(split_property[0], []).extend(value_list)

        # b. Multiple property key-value pairs should intersect.
        # To do this, filter on each agg_property_dict key.
        for req_property_key in agg_property_dict:
            filtered_data = filter_union(filtered_data,
                                        req_property_key,
                                        agg_property_dict[req_property_key])

        # Aggregate data of each model by properties.
        req_value_freq_dict = {}
        for req_prop in agg_property_dict:
            req_values_list = agg_property_dict[req_prop]
            req_value_sub_freq_dict = {}
            for req_value in req_values_list:
                req_value_count = 0
                for object in filtered_data:
                    for property_dict in object['properties']:
                        if property_dict['slug'] == req_prop:
                            compare_value = ''
                            if property_dict['type'] == 'string':
                                compare_value = str(req_value)
                            elif property_dict['type'] == 'integer':
                                compare_value = int(req_value)
                            else:
                                compare_value = bool(req_value)
                            if property_dict['value'] == compare_value:
                                req_value_count += 1
                req_value_sub_freq_dict[req_value] = req_value_count
            req_value_freq_dict[req_prop] = req_value_sub_freq_dict

    # Turn each value of req_value_freq_dict from dict to tuple.
    result = {}
    for freq_dict_key in req_value_freq_dict:
        freq_dict = req_value_freq_dict[freq_dict_key]
        for freq_key in freq_dict:
            result.setdefault(freq_dict_key, []).append((freq_key, freq_dict[freq_key]))

    # Sort tuples in descending order.
    for key in result:
        result[key].sort(key=lambda x: x[1], reverse=True)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # support `extend` action for python versions less than 3.8
    class ExtendAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            items = getattr(namespace, self.dest) or []
            items.extend(values)
            setattr(namespace, self.dest, items)

    parser.register("action", "extend", ExtendAction)
    parser.add_argument("-i", "--input-file", default="entities.json", help="The data file to be processed.")
    parser.add_argument(
        "-m",
        "--models",
        # stores a list, and extends each argument value to the list
        action="extend",
        default=list(),
        nargs="*",
        help="Model(s) to include.",
    )
    parser.add_argument(
        "-p",
        "--properties",
        # stores a list, and extends each argument value to the list
        action="extend",
        default=list(),
        nargs="*",
        help="""
        Properties to filter on.
        Assumes no key has ':' or ' ' and no property has ','. Format key:value1,value2
        """,
    )
    args = parser.parse_args()

    with open(args.input_file, "r") as f:
        data = json.load(f)

    pprint.pprint(run(data, args.models, args.properties))
