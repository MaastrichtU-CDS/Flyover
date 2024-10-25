import json
import logging
import os
import requests

# specify whether to do a 'dry-run', i.e., whether to actually post the query (wet) or just write queries (dry)
dry_run = True

# not a beauty but works
_database = 'databasename'
_variable_definition = 'localvariable'
_variable_predicate = 'PLACEHOLDER:variablepredicate'
_variable_class = 'PLACEHOLDER:variableclass'

_node_label = 'reconstructionlabel'
_node_class = 'PLACEHOLDER:reconstructionclass'
_node_aesthetic_label = 'reconstructionaestheticlabel'

_class_label = 'reconstructionlabel'
_class_predicate = 'PLACEHOLDER:reconstructionpredicate'
_class_class = 'PLACEHOLDER:reconstructionclass'
_class_aesthetic_label = 'reconstructionaestheticlabel'
_class_iri_label = 'reconstructioniri'

_prefixes_to_add = 'PREFIX PLACEHOLDER: <>'


def add_annotation(endpoint, database, prefixes, annotation_data, path, remove_has_column=False, save_query=True):
    """
    Add the annotation for a series of variables

    :param str endpoint: endpoint to add the mapping to
    :param str database: database to add the annotation to, e.g., db:'dataset'
    :param str prefixes: prefixes to add to the query
    :param dict/str annotation_data: dict with annotation data or path to JSON containing a dict can consist of
      "variable_info": {
        "identifier": {
          "predicate": "roo:P100061",
          "class": "ncit:C25364",
          "local_definition": "research_id"
        },
        "biological_sex": {
          "predicate": "roo:P100018",
          "class": "ncit:C28421",
          "local_definition": "biological_sex",
          "schema_reconstruction": [
            {
              "type": "class",
              "predicate": "roo:hassociodemographicvariable",
              "class": "mesh:D000091569",
              "class_label": "sociodemographicClass",
              "aesthetic_label": "Sociodemographic"
            }
          ],
          "value_mapping": {
            "terms": {
              "male": {
                "local_term": "0",
                "target_class": "ncit:C20197"
              },
              "female": {
                "local_term": "1",
                "target_class": "ncit:C16576"
              }
            }
          }
        },
        "age_at_diagnosis": {
          "predicate": "roo:hasage",
          "class": "ncit:C156420",
          "local_definition": "age_at_diagnosis",
          "schema_reconstruction": [
            {
              "type": "class",
              "predicate": "roo:hassociodemographicvariable",
              "class": "mesh:D000091569",
              "class_label": "sociodemographicClass",
              "aesthetic_label": "Sociodemographic information"
            },
            {
              "type": "node",
              "predicate": "roo:P100027",
              "class": "ncit:C29848",
              "node_label": "years",
              "aesthetic_label": "Years"
            }
          ]
        },
        "tumour_type": {
          "predicate": "roo:hastumourtype",
          "class": "ncit:C16899",
          "local_definition": "tumour_type",
          "schema_reconstruction": [
            {
              "type": "class",
              "predicate": "roo:hasclinicalvariable",
              "class": "ncit:C326200",
              "class_label": "clinicalClass",
              "aesthetic_label": "Clinical information"
            },
            {
              "type": "class",
              "predicate": "roo:P100029",
              "class": "ncit:C3262",
              "class_label": "neoplasmClass",
              "aesthetic_label": "Neoplasm"
            }
          ],
          "value_mapping": {
            "terms": {
              "1": {
                "local_term": "1",
                "target_class": "ncit:C16899"
              },
              "2": {
                "local_term": "2",
                "target_class": "ncit:C16899"
              },
              "3": {
                "local_term": "3",
                "target_class": "ncit:C16899"
              }
            }
          }
        },
        "tumour_stage": {
          "predicate": "roo:hastumourstage",
          "class": "ncit:C16899",
          "local_definition": "tumour_stage",
          "schema_reconstruction": [
            {
              "type": "class",
              "predicate": "roo:hasclinicalvariable",
              "class": "ncit:C326200",
              "class_label": "clinicalClass",
              "aesthetic_label": "Clinical information"
            },
            {
              "type": "class",
              "predicate": "roo:P100029",
              "class": "ncit:C3262",
              "class_label": "neoplasmClass",
              "aesthetic_label": "Neoplasm"
            }
          ],
          "value_mapping": {
            "terms": {
              "1": {
                "local_term": "1",
                "target_class": "ncit:C125472"
              },
              "2": {
                "local_term": "2",
                "target_class": "ncit:C125474"
              },
              "3": {
                "local_term": "3",
                "target_class": "ncit:C125476"
              },
              "4": {
                "local_term": "4",
                "target_class": "ncit:C125478"
              }
            }
          }
        }
      }
    :param str path: path to the directory where queries should be saved and where the optional JSON file is located
    :param bool remove_has_column: whether to remove the has_column predicate for cleanliness in the graph
    :param bool save_query: whether to save the query generated for the mapping
    """
    if dry_run:
        logging.info('Please not that this was specified as dry run in miscellaneous.py; '
                     'no queries will be posted to the endpoint.')

    construction_queries = None
    value_mapping_queries = None
    construction_success = True

    if isinstance(annotation_data, str) and isinstance(path, str):
        annotation_data = read_file(file_name=annotation_data, path=path)

    if isinstance(annotation_data, dict) is False:
        logging.warning('Annotation data incorrectly formatted, please see function docstring for an example.')
        return None

    for generic_category, variable_data in annotation_data.items():
        # object to trace the number of added components
        components = 0

        # specify certain components to remove, e.g., dbo:has_column or other predicates
        components_to_remove = {}
        removal_queries = {}

        # retrieve standard information
        predicate = variable_data.get('predicate')
        class_object = variable_data.get('class')
        local_definition = variable_data.get('local_definition')

        # break the loop if annotation data is not properly defined
        if not all(isinstance(var, str) for var in (predicate, class_object, local_definition)):
            logging.warning(f'Annotation data for variable {generic_category} is incorrectly formatted, '
                            f'please see function docstring for an example.')
            break

        # list to store the items that should be added to the query
        classes_insertion = ''
        classes_where = ''
        nodes_insertion = ''

        # check if schema reconstruction was defined
        reconstruction_data = variable_data.get('schema_reconstruction')
        if isinstance(reconstruction_data, list):
            construction_queries = {}

            for reconstruction in reconstruction_data:
                # when the reconstruction class is specified as class, create add a class to the datatable
                if 'class' in reconstruction.get('type'):
                    class_predicate = reconstruction.get('predicate')
                    class_class_object = reconstruction.get('class')
                    class_label = reconstruction.get('class_label')
                    class_aesthetic_label = reconstruction.get('aesthetic_label')

                    # break the loop if schema reconstruction is necessary but not properly defined
                    if not all(isinstance(var, str) for var in (
                            class_predicate, class_class_object, class_label, class_aesthetic_label)):
                        logging.warning(
                            f'Schema reconstruction data for variable {generic_category} is incorrectly formatted, '
                            'please see function docstring for an example.')
                        break

                    components += 1

                    # first construct the extra class
                    construction_response, construction_query \
                        = _construct_extra_class(endpoint=endpoint,
                                                 prefixes=prefixes,
                                                 database_name=database,
                                                 class_predicate=class_predicate,
                                                 class_class_object=class_class_object,
                                                 class_label=class_label,
                                                 class_aesthetic_label=class_aesthetic_label.capitalize(),
                                                 class_iri_label=class_aesthetic_label.lower().replace(' ', '_'))

                    # check whether the statement has been added
                    if dry_run is False:
                        construction_success = check_data_class(endpoint=endpoint, database_name=database,
                                                                class_label=class_label,
                                                                variable=generic_category,
                                                                response=construction_response)
                    else:
                        construction_success = True

                    # only proceed if extra class was successfully added
                    if construction_success:
                        construction_queries.update({class_aesthetic_label: construction_query})

                        # add to existing insertion and where strings
                        if components == 1:
                            classes_insertion = f'{class_predicate} ?component{components}.\n\n        '
                        else:
                            # if there already is an added component, associate the component with that first component
                            classes_insertion = (f'{classes_insertion}'
                                                 f'?component{components - 1} {class_predicate} ?component{components}.'
                                                 f'\n\n        ')

                            # additional classes should not be connected to the main component
                            components_to_remove.update({class_label: class_predicate})

                        classes_where = (f'{classes_where}'
                                         f'?tablerow {class_predicate} ?component{components} .'
                                         f'\n\n    '
                                         f'?component{components} rdf:type db:{database}.{class_label} .'
                                         f'\n\n\n    ')

                # when the reconstruction class is specified is node, create a 'hollow' node for the schema
                elif 'node' in reconstruction.get('type'):
                    node_predicate = reconstruction.get('predicate')
                    node_class = reconstruction.get('class')
                    node_label = reconstruction.get('node_label')
                    node_aesthetic_label = reconstruction.get('aesthetic_label')

                    # break the loop if schema reconstruction is necessary but not properly defined
                    if not all(isinstance(var, str) for var in (
                            node_predicate, node_class, node_label,
                            node_aesthetic_label)):
                        logging.warning(
                            f'Schema reconstruction data for variable {generic_category} is incorrectly formatted, '
                            'please see function docstring for an example.')
                        break

                    construction_response, construction_query \
                        = _construct_extra_node(endpoint=endpoint, database_name=database, prefixes=prefixes,
                                                node_label=node_label, node_class=node_class,
                                                node_aesthetic_label=node_aesthetic_label.capitalize())

                    # check whether the statement has been added
                    if dry_run is False:
                        construction_success = check_data_class(endpoint=endpoint, database_name=database,
                                                                class_label=node_label,
                                                                variable=generic_category,
                                                                response=construction_response)
                    else:
                        construction_success = True

                    # only proceed if extra class was successfully added
                    if construction_success:
                        construction_queries.update({node_aesthetic_label: construction_query})

                        # if no classes should be added, the 'zeroth' component is the variable the node should succeed
                        if len(classes_insertion) == 0:
                            nodes_insertion = (f'{nodes_insertion}\n\n        '
                                               f'?component1 {node_predicate} db:{database}.{node_label}.')

                        # if classes are added, the component number is the last class and not the variable; thus +1
                        else:
                            nodes_insertion = (f'{nodes_insertion}\n\n        '
                                               f'?component{components + 1} {node_predicate} db:{database}.{node_label}.')

        if construction_success:
            # add the annotation with specified reconstructions
            response, query = _add_annotation(endpoint=endpoint, variable=generic_category,
                                              database_name=database, prefixes=prefixes,
                                              local_definition=local_definition,
                                              predicate=predicate, class_object=class_object,
                                              classes_insertion=classes_insertion, classes_where=classes_where,
                                              nodes_insertion=nodes_insertion, components=components)

            # check whether the annotation was added successfully
            if dry_run is False:
                annotation_success = check_predicate(endpoint=endpoint, predicate=predicate,
                                                     variable=generic_category,
                                                     response=response)
            else:
                annotation_success = True

            # add the value mapping if necessary and possible
            if annotation_success and isinstance(variable_data.get('value_mapping'), dict):
                value_mapping_responses, value_mapping_queries = add_mapping(endpoint=endpoint,
                                                                             variable=generic_category,
                                                                             super_class=class_object,
                                                                             value_map=variable_data['value_mapping'])

            # remove the 'has_column' section
            if remove_has_column:
                components_to_remove.update({local_definition: 'dbo:has_column'})

            # remove all specified components
            for label, component in components_to_remove.items():
                response, removal = _remove_component(endpoint=endpoint, database_name=database, prefixes=prefixes,
                                                      local_variable=label, component_to_remove=component)
                # store the removal queries for saving
                removal_queries.update({label: removal})

        if save_query:
            if os.path.exists(os.path.join(path, 'generated_queries')) is False:
                os.mkdir(os.path.join(path, 'generated_queries'))

            if os.path.exists(os.path.join(path, 'generated_queries', generic_category)) is False:
                os.mkdir(os.path.join(path, 'generated_queries', generic_category))

            write_file(f'{generic_category}', query,
                       os.path.join(path, 'generated_queries', generic_category), '.rq')

            if isinstance(construction_queries, dict):
                for aesthetic_label, construction_query in construction_queries.items():
                    write_file(f'{generic_category}_schema_reconstruction_{aesthetic_label}', construction_query,
                               os.path.join(path, 'generated_queries', generic_category), '.rq')

            if isinstance(value_mapping_queries, dict):
                if os.path.exists(os.path.join(path, 'generated_queries', generic_category, 'mappings')) is False:
                    os.mkdir(os.path.join(path, 'generated_queries', generic_category, 'mappings'))

                for term, value_mapping_query in value_mapping_queries.items():
                    write_file(f'{generic_category}_value_mapping_term_{term}', value_mapping_query,
                               os.path.join(path, 'generated_queries', generic_category, 'mappings'), '.rq')

            if isinstance(removal_queries, dict) and len(removal_queries) > 0:
                if os.path.exists(os.path.join(path, 'generated_queries', generic_category, 'removals')) is False:
                    os.mkdir(os.path.join(path, 'generated_queries', generic_category, 'removals'))

                for label, removal in removal_queries.items():
                    write_file(f'{generic_category}_removal_in_{label}', removal,
                               os.path.join(path, 'generated_queries', generic_category, 'removals'), '.rq')

        components = 0
        components_to_remove = {}
        removal_queries = {}
        classes_insertion = ''
        classes_where = ''
        nodes_insertion = ''
        construction_queries = None
        value_mapping_queries = None
        query = None
        response = None
        construction_success = True
        annotation_success = None


def add_mapping(endpoint, variable, super_class, value_map):
    """
    add a mapping between various classes

    :param str endpoint: endpoint to add the mapping to
    :param str variable: variable name for logging purposes
    :param str super_class: class to add the mapping to
    :param dict value_map: dictionary containing the mapping data to save the query generated for the mapping
    """
    responses = []
    queries = {}

    if isinstance(value_map.get('terms'), dict):
        for term, term_data in value_map['terms'].items():
            target_class = term_data.get('target_class')
            local_term = term_data.get('local_term')

            if not all(isinstance(var, str) for var in (target_class, local_term)):
                logging.warning(
                    f'Value mapping for term {term} of variable {variable} is incorrectly formatted, '
                    'please see function docstring for an example.')
                break

            # call your add_mapping function with the appropriate arguments
            response, query = _add_mapping(endpoint=endpoint,
                                           target_class=target_class, super_class=super_class, local_term=local_term)

            # store response and query in a list
            responses.append(response)
            queries.update({term: query})

        return responses, queries
    else:
        logging.warning(f'Value map for variable {variable} is incorrectly defined, '
                        f'please see function docstring for an example.')
        return None, None


def check_data_class(endpoint, database_name, class_label, variable, response):
    """
    check whether a predicate has been added to the endpoint

    :param str endpoint: endpoint to add the mapping to
    :param str database_name: _database to add the annotation to, e.g., db:dataset
    :param str class_label: label to associate with the extra class e.g., neoplasmClass
    :param str variable: variable name for logging purposes
    :param requests.response response: response object from the Requests library
    :return:
    """
    if 200 <= response.status_code < 300:
        response, check_query = _check_for_data_class(endpoint=endpoint, database_name=database_name,
                                                      class_label=class_label)
        _response = json.loads(response.text)
        if _response.get('boolean', True) is True:
            logging.info(
                f'Class {class_label} was successfully annotated for {variable}.')
            return True

        else:
            logging.warning(
                f'Query for {variable} was successfully run on endpoint {endpoint}, '
                f'but class {class_label} was not found, consider checking the query.')
            return False

    else:
        logging.warning(
            f'Query for {variable}, was not successfully run on endpoint: {endpoint}.\n'
            f'HTTP response code: {response.status_code}.')
        return False


def check_predicate(endpoint, predicate, variable, response):
    """
    check whether a predicate has been added to the endpoint

    :param str endpoint: endpoint to add the mapping to
    :param str predicate: predicate to check
    :param str variable: variable name for logging purposes
    :param requests.response response: response object from the Requests library
    :return:
    """
    if 200 <= response.status_code < 300:
        response, check_query = _check_for_predicate(endpoint=endpoint, predicate=predicate)
        _response = json.loads(response.text)
        if _response.get('boolean', True) is True:
            logging.info(
                f'Predicate {predicate} was successfully annotated for {variable}.')
            return True

        else:
            logging.warning(
                f'Query for {variable} was successfully run on endpoint {endpoint}, '
                f'but predicate {predicate} was not found, consider checking the query.')
            return False

    else:
        logging.warning(
            f'Query: {variable}, was not successfully run on endpoint: {endpoint}.\n'
            f'HTTP response code: {response.status_code}.')
        return False


def read_file(file_name, path=None):
    """
    read a file and store its contents

    :param str file_name: name of the file to read
    :param str path: (optional) folder in which the file is located, defaults to current working directory
    :return: contents of the file (string or dictionary)
    """
    # default to the current working directory in case no path is specified
    if isinstance(path, str) is False:
        path = os.getcwd()

    file_path = os.path.join(path, file_name)

    # try to read the file and raise an error if unsuccessful
    try:
        logging.debug(f'Reading file {file_path}')
        with open(file_path, 'r') as file:
            if file_name.lower().endswith('.json'):
                # if the file has a .json extension, treat it as a JSON file
                file_contents = json.load(file)
            else:
                # otherwise, treat it as a text file
                file_contents = file.read()
            return file_contents
    except FileNotFoundError:
        logging.error(f'Error: File {file_path} not found.')
    except Exception as e:
        logging.error(f'An error occurred: {e} when attempting to read file {file_path}.')


def write_file(file_name, content, path=None, file_extension=None):
    """
    Write content to a file.

    :param str file_name: Name of the file to be written
    :param str content: Content to be written to the file
    :param str path: (optional) Folder in which the file should be saved, defaults to current working directory
    :param str file_extension: (optional) Extension of the file to be written
    """
    # default to the current working directory if no path is specified
    if path is None:
        path = os.getcwd()

    file_path = os.path.join(path, f'{file_name}{file_extension}')

    # try to write to the file and raise an error if unsuccessful
    try:
        logging.debug(f'Writing file {file_path}')
        with open(file_path, 'w') as file:
            file.write(content)
        logging.debug(f'File {file_path} successfully written.')
    except Exception as e:
        logging.error(f'An error occurred while writing the file: {e}')


def _add_annotation(endpoint, prefixes, variable, database_name, local_definition, predicate, class_object,
                    classes_insertion, classes_where, nodes_insertion, components, template_file=None):
    """
    Directly add an annotation

    :param str endpoint: endpoint to add the mapping to
    :param str prefixes: prefixes to add to the query
    :param str variable: name of the variable for logging purposes
    :param str database_name: _database to add the annotation to, e.g., db:dataset
    :param str local_definition: variable name to annotate, e.g., biological_sex
    :param str predicate: predicate to associate with the variable
    :param str class_object: class to associate with the variable
    :param str classes_insertion: string to insert the classes
    :param str classes_where: string to insert the classes in the where clause
    :param str nodes_insertion: string to insert the nodes
    :param str template_file: file name of the template, e.g., src/sparql_templates/template_mapping.rq
    :return: response from request
    """
    logging.debug(f'Adding standard annotation for {variable} to endpoint {endpoint}, database {database_name}')

    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates', 'template_annotation.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # the classes should precede the variable if present
    if components > 0:
        # if there already are more components, the variable will carry the number of components +1
        variable_component_name = f'?component{components + 1}'
        variable_insertion = f'?component{components} {predicate} {variable_component_name}.'
    else:
        components = 1
        variable_component_name = f'?component{components}'
        variable_insertion = f'{predicate} {variable_component_name}.'

    variable_where = (f'?tablerow dbo:has_column {variable_component_name} .\n\n    '
                      f'{variable_component_name} rdf:type db:{database_name}.{local_definition} .')

    # concatenate the insertions and where statements in a logical order
    full_insertion = f'{classes_insertion}{variable_insertion}{nodes_insertion}'
    full_where = f'{classes_where}{variable_where}'

    # establish what components go where
    replacements = {f'{_variable_predicate} ?component0.': full_insertion,
                    '?tablerow dbo:has_column ?component0 .\n\n    '
                    f'?component0 rdf:type db:{_database}.{_variable_definition} .': full_where,
                    _database: database_name,
                    _variable_definition: local_definition,
                    _variable_class: class_object,
                    _prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # run the query
    response = __post_query(endpoint, query)

    return response, query


def _add_mapping(endpoint, prefixes, target_class, super_class, local_term, template_file=None):
    """
    directly add a mapping between various classes and data-specific term

    :param str endpoint: endpoint to add the mapping to
    :param str prefixes: prefixes to add to the query
    :param str target_class: specific class, e.g., male or female
    :param str super_class: overarching class, e.g., biological sex
    :param str local_term: a value in the data e.g., 0 for females and 1 for males
    :param str template_file: file name of the mapping template, e.g., template_mapping.rq
    :return: response from request
    """
    logging.debug(f'Adding mapping for {super_class} to endpoint {endpoint}')

    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates', 'template_mapping.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # Create a dictionary to store the prefixes and their URIs
    prefix_to_uri = {}

    # Split the query into lines
    lines = query.split("\n")

    # Iterate over each line
    for line in lines:
        # Check if the line is a prefix declaration
        if line.startswith("PREFIX"):
            # Split the line into words
            words = line.split()

            # The second word is the prefix (remove the trailing ':')
            prefix = words[1][:-1]

            # The third word is the URI (remove the '<' and '>')
            uri = words[2][1:-1]

            # Store the prefix and URI in the dictionary
            prefix_to_uri[prefix] = uri

    # Extract the prefix from the target_class and super_class
    target_class_prefix = target_class.split(":")[0] if ":" in target_class else target_class
    super_class_prefix = super_class.split(":")[0] if ":" in super_class else super_class

    target_class = target_class.replace(":", "")
    super_class = super_class.replace(":", "")

    # Replace the prefix with the URI in the target_class and super_class
    if target_class_prefix in prefix_to_uri:
        target_class = target_class.replace(target_class_prefix, prefix_to_uri[target_class_prefix])

    if super_class_prefix in prefix_to_uri:
        super_class = super_class.replace(super_class_prefix, prefix_to_uri[super_class_prefix])

    # add the target_class, super_class, and local_term
    query = query % (target_class, super_class, local_term)

    # replace the placeholders
    replacements = {_prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # run the query
    response = __post_query(endpoint, query)

    return response, query


def _check_for_data_class(endpoint, database_name, prefixes, class_label, template_file=None):
    """
    check whether a statement has been added

    :param str endpoint: endpoint to add the mapping to
    :param str database_name: predicate to check whether present
    :param str prefixes: prefixes to add to the query
    :param str template_file: file name of the template, e.g., src/sparql_templates/template_mapping.rq
    :return: response from request
    """
    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates',
                                     'quality_control', 'template_to_check_class.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # replace the placeholders
    replacements = {_database: database_name,
                    _class_label: class_label,
                    _prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # run the query
    response = __post_query(endpoint=endpoint.rsplit('/statements', 1)[0], query=query,
                            data_style=f'query=' + query)

    return response, query


def _check_for_predicate(endpoint, prefixes, predicate, template_file=None):
    """
    check whether a statement has been added

    :param str endpoint: endpoint to add the mapping to
    :param str prefixes: prefixes to add to the query
    :param str predicate: predicate to check whether present
    :param str template_file: file name of the template, e.g., src/sparql_templates/template_mapping.rq
    :return: response from request
    """
    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates',
                                     'quality_control', 'template_to_check_predicate.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # replace the placeholders
    replacements = {_variable_predicate: predicate,
                    _prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # remove the prefix for cleanliness
    query.replace(_prefixes_to_add, '')

    # run the query
    response = __post_query(endpoint=endpoint.rsplit('/statements', 1)[0], query=query,
                            data_style=f'query=' + query)

    return response, query


def _construct_extra_class(endpoint, database_name, prefixes, class_label, class_predicate, class_class_object,
                           class_aesthetic_label, class_iri_label,
                           template_file=None):
    """
    Directly add an annotation with an extra class

    :param str endpoint: endpoint to add the mapping to
    :param str database_name: _database to add the annotation to, e.g., db:dataset
    :param str prefixes: prefixes to add to the query
    :param str class_label: label to associate with the extra class e.g., neoplasmClass
    :param str class_predicate: predicate to associate with the extra class
    :param str class_class_object: class to associate with the extra class
    :param str class_aesthetic_label: label to associate with the extra class, e.g., Neoplasm
    :param str class_iri_label: iri label to associate with the extra class e.g., neoplasm
    :param str template_file: file name of the template, e.g., src/sparql_templates/template_mapping.rq
    :return: response from request
    """
    logging.debug(f'Constructing extra {class_label} to endpoint {endpoint}, database {database_name}')

    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates',
                                     'schema_reconstruction', 'template_for_extra_class.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # replace the components
    replacements = {_database: database_name,
                    _class_label: class_label,
                    _class_predicate: class_predicate,
                    _class_class: class_class_object,
                    _class_aesthetic_label: class_aesthetic_label,
                    _class_iri_label: class_iri_label,
                    _prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # remove the prefix for cleanliness
    query.replace(_prefixes_to_add, '')

    # run the query
    response = __post_query(endpoint, query)

    return response, query


def _construct_extra_node(endpoint, database_name, prefixes, node_label, node_class, node_aesthetic_label,
                          template_file=None):
    """
    Add an extra node that can be used to associate to an actual variable

    :param str endpoint: endpoint to add the mapping to
    :param str database_name: _database to add the annotation to, e.g., db:dataset
    :param str prefixes: prefixes to add to the query
    :param str node_label: label to associate with the extra class e.g., neoplasmClass
    :param str node_class: class to associate with the extra node
    :param str node_aesthetic_label: label to associate with the extra class, e.g., Neoplasm
    :param str template_file: file name of the template, e.g., src/sparql_templates/template_mapping.rq
    :return: response from request
    """
    logging.debug(f'Adding extra node for {node_label} to endpoint {endpoint}, database {database_name}')

    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates',
                                     'schema_reconstruction', 'template_for_extra_node.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # replace the components
    replacements = {_database: database_name,
                    _node_label: node_label,
                    _node_class: node_class,
                    _node_aesthetic_label: node_aesthetic_label,
                    _prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # remove the prefix for cleanliness
    query.replace(_prefixes_to_add, '')

    # run the query
    response = __post_query(endpoint, query)

    return response, query


def _remove_component(endpoint, database_name, prefixes, local_variable, component_to_remove='dbo:has_column',
                      template_file=None):
    """
    remove a component such as dbo:has_column for a certain local variable name

    :param str endpoint: endpoint to add the mapping to
    :param str database_name: _database to add the annotation to, e.g., db:dataset
    :param str prefixes: prefixes to add to the query
    :param str local_variable: variable name to annotate, e.g., biological_sex
    :param str component_to_remove: component to remove, e.g., dbo:has_column
    :param str template_file: file name of the template, e.g., src/sparql_templates/template_mapping.rq
    :return: response from request
    """
    logging.debug(f'Remove has_column for {local_variable} on endpoint {endpoint}, database {database_name}')

    if isinstance(template_file, str) is False:
        path = os.getcwd()
        if 'annotation_helper' not in path:
            path = os.path.join(path, 'annotation_helper')
        template_file = os.path.join(path, 'src', 'sparql_templates',
                                     'schema_reconstruction', 'template_to_remove_component.rq')

    # retrieve the mapping template
    query = read_file(template_file)

    # replace components
    replacements = {_database: database_name,
                    _variable_definition: local_variable,
                    'dbo:has_column': component_to_remove,
                    _prefixes_to_add: prefixes,
                    '# Template that is automatically filled using Python.':
                        '# This query was automatically generated using the annotation helper.'}

    for old, new in replacements.items():
        query = query.replace(old, new)

    # run the query
    response = __post_query(endpoint, query)

    return response, query


def __post_query(endpoint, query, headers=None, data_style=None):
    """
    run a query and return the response

    :param str endpoint: endpoint to post the provided query to
    :param str query: query that is posted to the provided endpoint
    :param dict headers: provide the headers to use when posting request
    e.g., {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
    :return:
    """
    if isinstance(headers, dict) is False:
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    if isinstance(data_style, str) is False:
        data_style = 'update=' + query

    if dry_run is False:
        annotation_response = requests.post(endpoint, data=data_style, headers=headers)
    else:
        annotation_response = 'not-a-http-response'

    return annotation_response
