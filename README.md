# FLYOVER

## Introduction

We have built a dockerized Data FAIR-ification tool that takes clinical datasets and converts them into Resource
Descriptor Format (RDF).
This conversion is done by an application which parses an entire structured table as a simple
flattened RDF object.
This so-called "Triplifier" tool works with PostGreSQL and CSV tables.

For user data, a new module (data_descriptor) is created where the user can describe their own data and provide us with
the metadata, which can then be used to create annotations.

For a detailed explanation and a practical example of this tool, please refer to the following published paper:
https://academic.oup.com/bjrai/article/1/1/ubae005/7623642#445743545 (open access)

## Components

### 1. Data Descriptor Module

A simple graphical interface tool for helping a local user to describe their own data (in the form of CSV or
PostGreSQL).
On uploading the data, Triplifier runs and converts this data into RDF triples which is then uploaded to
the RDF store, along with an OWL file.
The next page displays the list of columns and prompts the user to give some
basic information about their data which is then added back to the OWL file in the RDF store.

#### How to run?

Clone the repository (or download) on your machine.
On windows, please use WSL2 with Docker, on macOS/Linux, you can use docker directly.
For the complete workflow, please execute the following commands from the project folder:

```
docker-compose up -d
```

You can find the following systems:

* Web interface for data upload: [[http://localhost:5000]]
* RDF repository: [[http://localhost:7200]]

#### Publishing anonymous METADATA

The user can publish their OWL files to a private cloud repository, which can then be used to create a customised
annotation graph for their data.
The usage of metadata for the creation of annotations ensures the privacy of user data.

### 2. Annotation helper script

After the user has described their data using the Data Descriptor Module, they can use the metadata to create
annotations.
For that, the annotation helper script takes user specified metadata and transforms it into variable-level annotation
queries that are sent to the RDF repository.

This means that you can specify variable level metadata in a JSON file and the script will create the annotations for
them.

The script uses various SPARQL template files in which Python will fill in the necessary information from the metadata
file.

#### How to run?

##### Specification of metadata

Specify the metadata in the `data_semantic_map.json` file.

The basic metadata should be in the following format; here with an example of a variable for biological sex:

```
{
  "endpoint": "http://localhost:7200/repositories/userRepo/statements",
  "database_name": "my_database",
  "variable_info": {
    "biological_sex": {
      "predicate": "sio:SIO_000008",
      "class": "ncit:C28421",
      "local_definition": null
  }
}
```

The variable info can be also expanded with a `schema_reconstruction` and `value_mapping` field.

###### Specifying a schema reconstruction

This field can be used to reconstruct the schema, and can be used to create an extra class (e.g., to cluster variables),
or an extra node (e.g., to specify the unit of a variable).

The previous example is expanded as such

      "schema_reconstruction": [
        {
          "type": "class",
          "predicate": "sio:SIO_000235",
          "class": "mesh:D000091569",
          "class_label": "demographicClass",
          "aesthetic_label": "Demographic"
        }
      ]

Note that you can add multiple schema reconstructions, these will be added to the graph in the same order as they are
specified (from top to bottom).  
The schema reconstruction of type 'class' also takes a placement argument - either 'before' or 'after' -
which specifies the location of the new class to be added.
Placement defaults to 'before'.   
'Before' refers to a class that is placed between an individual's class (e.g. a patient or participant), and the
variable class.  
'After' refers to a class that is placed between the variable class and the class the reconstruction itself is referring
to.

An example of multiple schema reconstructions for a single variable is provided in the
example data.

###### Specifying your value mapping

This field can be used to map your values to a specific terminology, e.g., specify what value represents males and what
value represents females.

The previous example is expanded as such

      "value_mapping": {
        "terms": {
          "male": {
            "local_term": null,
            "target_class": "ncit:C20197"
          },
          "female": {
            "local_term": null,
            "target_class": "ncit:C16576"
          }
        }
      }

###### Complete example of metadata for biological sex

Below, we display the complete example of how the semantic map entry for the class 'biological_sex' could look like:

    "biological_sex": {
      "predicate": "sio:SIO_000008",
      "class": "ncit:C28421",
      "local_definition": null,
      "schema_reconstruction": [
        {
          "type": "class",
          "predicate": "sio:SIO_000235",
          "class": "mesh:D000091569",
          "class_label": "demographicClass",
          "aesthetic_label": "Demographic"
        },
        {
          "type": "class",
          "placement": "after",
          "predicate": "sio:SIO_000253",
          "class": "ncit:C142529",
          "class_label": "ehrClass",
          "aesthetic_label": "EHR"
        }
      ],
      "value_mapping": {
        "terms": {
          "male": {
            "local_term": null,
            "target_class": "ncit:C20197"
          },
          "female": {
            "local_term": null,
            "target_class": "ncit:C16576"
          },
          "intersex": {
            "local_term": null,
            "target_class": "ncit:C45908"
          },
          "missing_or_unspecified": {
            "local_term": null,
            "target_class": "ncit:C54031"
          }
        }
      }
    }

#### Running the script

After specifying the metadata in JSON, you can fill in local terminology and run the script with the following command
from the repository folder:

```
python triplifier/data_descriptor/annotation_helper/main.py 
```

#### Evaluate the annotation process

By default, the script will log the annotation process and save the generated SPARQL queries in `.rq` files.

The log file can be found as `annotation_log.txt` and the queries that have been created can be found in
the `generated_queries` folder per specified variable.
Both located in the same folder as your JSON metadata.

In case the log indicates that the annotation process was unsuccessful, please consider inspecting the generated queries
for those variables that were unsuccessfully annotated.

### Example data

In the `example_data` folder, you can find an example of a global semantic map in `data_semantic_map.json`.  
Additionally there is data (`.csv`) and a mapping file (`.json`) of two fictitious centres
(centre a, which uses English terminology, and centre b, which uses Dutch terminology).
These files can be used
to test the Data Descriptor Module and the Annotation Helper.
Additionally, this JSON metadata contains all supported
scenarios of  `schema_reconstruction`.
The example data is a synthetic dataset generated with the MOSTLY AI Data Intelligence Platform using the data semantic
maps as guide for data generation.

## Developers

- Varsha Gouthamchand
- Joshi Hogenboom
- Johan van Soest
- Leonard Wee


