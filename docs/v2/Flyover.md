# Introduction
This document provides an overview of the proposed idea how Flyover can work. The workflow steps are represented in the figure below. Flyover is a "wizard" tool to make data (more) FAIR, and convert source data into various CDM systems. The items in green are user actions, the items in black are automated system tasks, and the red items are optional LLM integrations.

![[flyover.excalidraw|100%]]

## Definition of terms
- Schema: the structural ordering of data
- Syntactic interoperability: the structural ordering of data, in a specific syntactical language or system.
- Semantic interoperability: the terminological mapping to existing terminology systems. To be truly semantically interoperable, terminologies *and* schema/syntax should be interoperable.

# Steps in process
Below we will present a small description of the steps in this tool.

## 1. Specify requirement file from project
Every research project will have a set of required data elements they want to collect. The project will define a set of variables, including terminology codes for both the variable as well as the (categorical) value. This is currently described in JSON (see [this example](https://github.com/MaastrichtU-CDS/Flyover/blob/copilot/add-manual-materialization-option/example_data/mapping_template.jsonld)), however in the future could be described in ShEx/SHACL or any other community adopted standard.
**User action:** provide the data requirements file, or a URL to this requirements file.

## 2. Upload data
After specifying the requirement file, the user can upload a dataset. Currently the system accepts CSV files, however the backend technology (triplifier) can also handle relational databases.
**User action:** upload a given CSV file

## 3. Extract dataset metadata
This is an automated action, where the following items are extracted from the CSV file. Currently these items are stored in an OWL file, where every table and column name is a class in a new OWL file. Classes inherit from classes `dbo:TableRow` and `dbo:TableCell`. For more information, see [this paper](http://ceur-ws.org/Vol-2849/#paper-11). We will extend `dbo:TableCell` with the count for unique values. However when we detect the column contains unique values (or a ratio of 7:10 for unique_values:number_rows), we will omit this from the frequency count. Numerical columns will result in statistics regarding mean, median, minimum and maximum values. In the new version, we will store these results in JSON(-LD), as not every converter (see step [[#6. Data converter]]) does not need OWL, only the terminology concepts.

## 4. Annotate column names
After the metadata extraction, users can identify which information elements from the project requirements file (see [[#1. Specify requirement file from project]]) matches column names in the provided and extracted dataset characteristics. This is current functionality in Flyover.
**User action:** for a given column, select the correct information from the list of requested information element.

## 5. Annotate categories
As a next step, flyover understands (from the project requirements) which information elements are categorical. For these categorical variables, a user can choose from the options provided in the project requirements. This is current functionality in Flyover.
**User action:** for a given value for an information element, select the correct terminology concept

## 6. Data converter
Now the tool annotated terminologies for the required columns, the data converter can use the annotations (see [[#4. Annotate column names]] and [[#5. Annotate categories]]) to convert data into a target syntactic standard and converting this directly in a semantic target standard.
Currently, Flyover converts into RDF, following the source schema structure (see the [triplifier paper](http://ceur-ws.org/Vol-2849/#paper-11)). However, we want include more options for direct conversion, which will be discussed below. In general, these can be implemented as Python scripts, which can be loaded into our system at configuration time. Creating flexibility in the conversion layer to use completely custom Python code, or reuse existing Python libraries.

### YARRRML RDF converter
This conversion is based on RML, which is a superset of R2RML. R2RML and RML did not support value conversion (e.g. values of 0 or 1 to the appropriate SNOMED codes for the information element "biological sex"). YARRRML can overcome this issue by implementing custom functions which perform this value mapping.

### Default OMOP converter
Assuming users will upload CSV files with columns as information element names, we can build a python script which will read the source data, and perform SQL insert statements into the OMOP database at the appropriate place. This probably requires an extension of the requirement file in step 1.

### Custom (OMOP) converter
If the above OMOP converter fails, or if mapping to a different SQL schema is needed, users can generate a custom Python script for conversion. Hence, Flyover should be able to accept custom Python scripts using an API or file-based configuration on startup.

### FHIR converter
Similar to the (custom) OMOP converter, we can implement a FHIR converter, given JSON templates and filling the correct information in the various fields.

## 7. DCAT export
Based on the conversion process, we can promt a user to fill in specific criteria which are mandatory for Health DCAT-AP 3.0 `Dataset` and `Distribution` entities, and provide the user with a Turtle file (`.ttl`) which can be uploaded into an existing catalog (which accepts this format).
**User action**: provide the requested additional information, and download the DCAT information bundle from the tool.

# Use LLM AI in process

During this process, AI can be used in three stages:
- Exploring column annotatioons
- Exploring category annotations
- Generate / update conversion steps

These annotations can happen using local models (e.g. Gemma, Llama or qwen; 3b or 7b), or hosted modens (Anthropic, OpenAI, or GPT hosted within hospital tenant). The advantage is that API calls to these models are generic. Hence it would be easy to replace a local to a hosted model, or between providers of hosted models.

A detail for every stage is described below.

## Exploring column annotations
Based on the table and column names, together with the information elements stored in the requirements file, the LLM can provide suggestions for column annotations. Hence, the only information sent to the LLM is:
- Table names of source data
- Column names of source data
- information elements stored in requirements file.
Output:
- suggested terminology annotation for column names

The information of this LLM would be input for step [[#4. Annotate column names]].

## Exploring category annotations
Based on the annotations of step [[#4. Annotate column names]], we know which columns are categorical variables. For these categorical variables we will extract the unique values (where number of samples >5 or any other set threshold). We will ask the LLM to provide a suggestion for value annotations, where the following information is sent to the LLM:
- Column definition/annotation information (source column name, ontology term annotated)
- Unique values for this specific column (where number of samples > 5)
- Options for terminology values for this specific column, stored in requirements file
Output:
- suggested terminology annotations for column values

The information of this LLM would be input for step [[#5. Annotate categories]].

## Input for data converter
To adapt data converter python scripts, we can use the target schema, requirements file, source data schema information (column names & unique values) and annotation information.
To adapt a python script, a larger model is needed which is probably not feasible to run on a local laptop. Hence, we will use the LLM in this case outside of the Flyover application, where we manually will check for identifiable data in the LLM input, but also the generated python script. If this script is acceptable, this will be implemented into Flyover using an API call or user interface action.