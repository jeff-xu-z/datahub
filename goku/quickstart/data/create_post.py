#!/usr/bin/env python3

# Imports for urn construction utility methods
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def main(graph):
    result = graph.execute_graphql(query = f"""
mutation test {{
  createPost(
    input: {{
      postType: HOME_PAGE_ANNOUNCEMENT, 
      content: {{
        contentType: TEXT, 
        title: "JEFFXU: Planed Upgrade 2023-03-23 20:05 - 2023-03-23 23:05", 
        description: "datahub upgrade to v0.10.1"
      }}
    }}
  )
}}
""")
    print(result)

if __name__ == "__main__":
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))
    main(graph)
