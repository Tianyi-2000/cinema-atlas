# cinema-atlas
A multi-source data architecture project for film knowledge graphs, temporal popularity analytics, and audience behavior insights.

# Cinema Atlas

## Project Overview

Cinema Atlas is a data architecture project that explores film as a connected system of movies, people, genres, production companies, audience ratings, and temporal popularity signals. Instead of treating movies as isolated records, the project models cinema as an analytical platform where users can study relationships, trends, and cultural patterns across film data.

The project focuses on designing and implementing an end-to-end data pipeline that moves raw movie-related data into structured analytical tables. The system is intended to support both relationship-based analysis and time-based film analytics.

## Project Goal

The goal of Cinema Atlas is to build a scalable data architecture that can support analytical questions about the film industry, audience behavior, and movie relationships.

Example analytical questions include:

* How are movies connected through actors, directors, genres, production companies, and themes?
* How does movie popularity change over time?
* Which movies, people, or genres show strong audience engagement?
* Can relationship paths explain why two films are similar or connected?

This project emphasizes data modeling, ETL design, schema design, and cloud-based data processing.

## Data Sources

The project may use multiple public or API-based movie data sources, including:

* TMDB API for movie metadata, popularity, ratings, genres, and production information
* MovieLens for user rating data
* Wikipedia or Wikidata for additional movie, person, and relationship data
* Other public datasets for box office, reviews, or temporal popularity signals if needed

These sources may include structured data, semi-structured JSON data, and text-based data.

## Architecture Plan

The planned architecture follows a lakehouse-style pipeline:

1. Raw data is collected from APIs or public datasets.
2. The bronze layer stores raw or lightly processed source data.
3. The silver layer cleans, normalizes, and structures the data into usable tables.
4. The analytics layer supports fact and dimension tables for querying and presentation.
5. Pipeline orchestration may be handled through Airflow or a similar workflow tool.

Planned tools include:

* GitHub for code and documentation
* AWS S3 for cloud storage
* Databricks for data processing and table creation
* Delta tables or structured tables for cleaned analytical data
* Airflow for pipeline orchestration

## Current Status

This project is currently under development as part of a data architecture group project. The current focus is on setting up the repository, documenting the data ingestion process, inspecting raw JSON fields, and designing the first version of the silver layer schema.

## Repository Structure

The repository may include:

```text
docs/
  Project documentation and architecture notes

notebooks/
  Databricks or development notebooks

sql/
  Table creation scripts and queries

schemas/
  Bronze, silver, and analytics schema drafts

src/
  Source code for ingestion and transformation logic
```

## Team

Team members will be added here.
