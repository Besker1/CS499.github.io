from pymongo import MongoClient

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username='root', password='6Y1LFaNnsx'):
        USER = username
        PASS = password
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 32162
        DB = 'AAC'
        COL = 'animals'

        self.client = MongoClient(f'mongodb://{USER}:{PASS}@{HOST}:{PORT}')
        self.database = self.client[DB]
        self.collection = self.database[COL]
        self.create_indexes()

    # Creating indexes on frequently queried fields such as animal_type, breed, and 
    # age_upon_outcome_in_weeks can significantly improve the speed of database operations.

    def create_indexes(self):
        """Create indexes on frequently queried fields for optimization."""
        self.collection.create_index([('animal_type', 1)])
        self.collection.create_index([('breed', 1)])
        self.collection.create_index([('age_upon_outcome_in_weeks', 1)])
        print("Indexes created on 'animal_type', 'breed', and 'age_upon_outcome_in_weeks'")

    def create(self, data):
        if data is not None:
            insert_result = self.collection.insert_one(data)  # Insert a document
            return True if insert_result.acknowledged else False
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, query, batch_size=100):
        if query is not None:
            documents = self.collection.find(query).batch_size(batch_size)  # Batch processing
            return list(documents)
        else:
            raise Exception("Query parameter is empty")
        


    # For large datasets, implementing pagination will improve user experience and
    # prevent overwhelming the client with too much data.

    def read_with_pagination(self, query, page_number=1, page_size=100):
        if query is not None:
            skip = (page_number - 1) * page_size
            documents = self.collection.find(query).skip(skip).limit(page_size)
            return list(documents)
        else:
            raise Exception("Query parameter is empty")
        

    def update(self, query, update_data):
        if query and update_data:
            result = self.collection.update_many(query, {'$set': update_data})
            return result.modified_count
        else:
            raise Exception("Query and update_data parameters must not be empty")

    def delete(self, query):
        if query:
            result = self.collection.delete_many(query)
            return result.deleted_count
        else:
            raise Exception("Query parameter is empty")
        
    def filter_by_breeds(self, query, preferred_breeds):
        """Filter animals by preferred breeds using a set for fast lookup."""
        if query is not None:
            breed_set = set(preferred_breeds)
            documents = self.collection.find(query)
            filtered_animals = [doc for doc in documents if doc['breed'] in breed_set]
            return filtered_animals
        else:
            raise Exception("Query parameter is empty")