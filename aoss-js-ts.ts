// npm i @opensearch-project/opensearch @aws-sdk/credential-providers
import { Client } from '@opensearch-project/opensearch';
import { AwsSigv4Signer } from '@opensearch-project/opensearch/aws';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers'; 


export class OpenSearchService {
    // Instance variable to hold the MilvusClient object
    protected OpenSearchClient: Client | any;

    constructor(protected config: string) {
        console.log("Connecting to Opensearch");
        this.OpenSearchClient = new Client({
            node: this.config, // OpenSearch domain URL
            ...AwsSigv4Signer({
                region: AWS_REGION, //us-west-2
                service: 'aoss',
                getCredentials: fromNodeProviderChain(),
            }),
        });
        // Call an async function to verify the connection
        this.verifyConnection();

    }


    public async verifyConnection() {
        try {
            const index = "test";
            await this.OpenSearchClient.indices.exists({ index });
            console.log('Connected to Opensearch');
        } catch (error) {
            console.log(error);
        }
    }

    // Asynchronous method to list all collections in the current database
    async listCollection() {
        return new Promise(async (resolve, reject) => {
            try {
                // Retrieve the list of collection names from Milvus
                let listCollection = (await this.OpenSearchClient.cat.indices({ format: 'json' }));
                resolve(listCollection);
            } catch (error) {
                // Reject with the error if listing collections fails
                reject(error);
            }
        });
    }

    async drop(index_name: string) {
        return new Promise(async (resolve, reject) => {
            try {
                // Drop the specified collection and resolve if successful
                await this.OpenSearchClient
                    .indices.delete({ index: index_name })
                    .then(() => {
                        resolve({ message: `successfully deleted ${index_name}` });
                    })
                    .catch((err: any) => {
                        // Reject with the error if dropping collection fails
                        reject(err);
                    });
            } catch (error) {
                // Reject with the error if an exception occurs
                reject(error);
            }
        });
    }

    async createIndex(index_name: any) {
        return new Promise(async (resolve, reject) => {
            try {
                const indexSettings = {
                    settings: {
                        index: {
                            knn: true, // Enable KNN search
                        },
                    },
                    mappings: {
                        properties: {
                            langchain_vector: {
                                type: 'knn_vector', // Use 'dense_vector' for KNN search
                                dimension: 1536, // Ensure this matches the dimension of your embeddings
                            },
                            // langchain_primaryid: {
                            //   type: 'long', // Corresponds to Int64 in Milvus
                            //   index: true,  // Make sure this field is indexed
                            // },
                            langchain_text: {
                                type: 'text',
                                fields: {
                                    keyword: {
                                        type: 'keyword',
                                        ignore_above: 1800, // Set the maximum length for the keyword field
                                    },
                                },
                            },

                            langchain_source: {
                                type: 'text', // Similar to VarChar
                                fields: {
                                    keyword: {
                                        type: 'keyword',
                                        ignore_above: 20000, // Set a maximum length for the keyword field
                                    },
                                },
                            },
                            langchain_file_name: {
                                type: 'text', // Similar to VarChar
                                fields: {
                                    keyword: {
                                        type: 'keyword',
                                        ignore_above: 250, // Set a maximum length for the keyword field
                                    },
                                },
                            },
                        },
                    },
                };
                // Create a new collection with the provided configuration
                await this.OpenSearchClient
                    .indices.create({
                        index: index_name,
                        body: indexSettings,
                    })
                    .then(() => {
                        // console.log("index created");
                        resolve({ message: "successfully created" });
                    })
                    .catch((err: any) => {
                        // Reject with the error if collection creation fails
                        // console.log("err", err);
                        reject(err);
                    });
            } catch (error) {
                // Log and reject with the error if an exception occurs
                // console.log("error", error);
                reject(error);
            }
        });
    }

    async indexExists(index_name: string) {
        return new Promise(async (resolve, reject) => {
            try {
                // Resolve with the result of checking if the collection exists
                resolve(await this.OpenSearchClient.indices.exists({ index: index_name }));
            } catch (error) {
                // Reject with false if an error occurs (assuming false means the collection doesn't exist)
                resolve(false);
            }
        });
    }


    async insert(data: any) {
        return new Promise(async (resolve, reject) => {
            try {
                // Insert entities into the specified collection
                await this.OpenSearchClient.bulk({ body: data });

                // Resolve with a success message
                resolve({ message: "Successfully inserted data" });
            } catch (error) {
                // Log and reject with the error if an exception occurs
                console.log("error inserting documents", error);
                reject(error);
            }
        });
    }


    // Asynchronous method to delete entities (vectors) from a collection based on a file name
    async deleteEntities(collection_name: string, file_name: string) {
        return new Promise(async (resolve, reject) => {
            try {
                // Query for entities with the specified file name
                // console.log("in Delete Flow");
                // console.log("Before delete flow");
                const indicesInfo: any = await this.listCollection();
                const indices = indicesInfo.body
                //  console.log("indices", indices);
                const allIndices:any = [];
                for (const idx of indices) allIndices.push(idx.index);
                //  console.log("allIndices", allIndices);

                const searchQuery = {
                    size: 9999,  // Adjust the number of results as needed
                    query: {
                        term: {
                            "langchain_file_name.keyword": {
                                value: file_name
                            }
                        }
                    }
                };
                this.OpenSearchClient
                    .search({
                        index: allIndices.join(','),  // Search across all indices
                        body: searchQuery
                    })
                    .then(async (result: any) => {
                        //console.log("result",result)

                        // Extract IDs of entities matching the query
                        const ids =
                            result?.body?.hits?.hits?.map((row: any) => row._id) || [];

                        // console.log(ids);
                        for (let id of ids) {
                            // console.log("in for loop ")
                            const delResp = await this.OpenSearchClient.delete({
                                index: collection_name,
                                id: id
                            });
                            // console.log("delResp", delResp);
                        }
                        resolve("successfully deleted documents");

                        // Delete entities with the extracted IDs
                    })
                    .catch((err: any) => {
                        // Reject with the error if querying entities fails
                        console.log("Failed to Delete:", err);
                        reject(err);
                    });
            } catch (error) {
                // Reject with the error if an exception occurs
                console.log("Failed to Delete:", error);
                reject(error);
            }
        });
    }


    async performSearch(query: any, indices: any, numOfResults = 5) {
        try {
            // Get the query embedding
            const Embedding = await yourEmbeddingModel.generateEmbeddings(EMBEDDING_MODEL, query);
            const queryEmbedding= Embedding.data.data[0].embedding;
            // console.log("after queryEmbedding");
            // Define the search query
            const searchQuery = {
                size: numOfResults,
                query: {
                    knn: {
                        langchain_vector: {
                            vector: queryEmbedding,
                            k: numOfResults
                        }
                    }
                }
            };

            // Perform the search
            const { body: response } = await this.OpenSearchClient.search({
                // index: indices.join(','), // Use all specified indices
                index: indices,
                body: searchQuery
            });
            

            return response;
        } catch (error) {
            console.error('Error performing search:', error);
            throw error;
        }
    }


}