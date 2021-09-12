Input data - Recipes information with ingredients and preparation time and cook time information etc in JSON format.

Sample - {"name": "Creamy Cheese Grits with Chilies", "ingredients": "4-1/2 cups Water\n1/2 teaspoon Salt\n1 cup Grits (quick Or Regular)\n1/2 can (10 Ounce Can) Rotel (tomatoes And Chilies)\n1 can (4 Ounce Can) Chopped Green Chilies\n8 ounces, weight Monterey Jack Cheese, Grated\n4 ounces, weight Cream Cheese, Cut Into Cubes\n1/4 teaspoon Cayenne Pepper\n1/4 teaspoon Paprika\n Black Pepper To Taste\n1 whole Egg Beaten", "url": "http://thepioneerwoman.com/cooking/2010/10/creamy-cheese-grits-with-chilies/", "image": "http://static.thepioneerwoman.com/cooking/files/2010/10/5079611293_ff628b6e0c_z.jpg", "cookTime": "PT45M", "recipeYield": "8", "datePublished": "2010-10-14", "prepTime": "PT5M", "description": "I have a good, basic recipe for cheese grits in my cookbook, but last night I was feeling feisty.     I was cooking steaks. B..."}

Project Requirement:

-Load data from source into intermediate location in parquet format and do necessary data cleaning.
-Read data from intermediate location and perform below transformations:
	-Filter recipes with Beef as one of the ingredient
	-Calculate total time taken to cook the recipe (prepTime and cookTime). Source Time Format (PT + Hours + H + Minutes + M)
	-Based on total cook time, classify recipe to be "easy" if total time < 30 mins, "medium" if total time is in between 30-60 mins and "difficult" if total time) > 60 mins
	-Save resulting dataset with 2 columns - total time and complexity level into csv format.



Project details:

There are 4 python files with code and 1 config file with pipeline parameters:
1. ingest_data - used to read raw data, refine raw data and save it.
2. tranform - where the tranformation logic is written
3. spark_functions - Some common functions and UDFs are defined in the file
4. recipe_pipeline - file which have the main function and run_pileline method to run the pipeline

Workflow:
Step1 - Raw data is read from input folder and refined with below logic:
	- Remove records where prep time and cook time both are are blank as this will not be valid use case
	- select only required columns - ingredients, prepTime, cookTime

Step2 - refined data is saved in output/curated folder in parquet format for further processing.

Step3 - refined data from Step2 is used to perform the transformation logic as below:
	-UDF calculate_time is used to get time in minutes for cookTime and prepTime in new column cook_time_mins and prep_time_mins respectively
	-addition of cook_time_mins and prep_time_mins is stored in new column avg_total_cooking_time.
	-UDF calculate_complexity to calculate complexity category based on avg_total_cooking_time in new column difficulty
	
Step4 - Output of step3 is stored in csv format in output/processed folder

Other files:
1. Schema for raw data is stored in schemas folder and used to when reading raw data to save processing time.
2. Logs folder have log files 
3. output/curated - where intermediate data is stored parquet format
4. output/processed - final output is stored in csv format 
5. config.ini - contains input output paths, file formats info etc

