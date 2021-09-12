from pyspark.sql.types import StructType
import json

class spark_utils:

    def get_json_schema(self, file_path):
        f = open(file_path, )
        data = json.load(f)
        new_schema = StructType.fromJson(data)
        return new_schema

    # UDF to calculate time in minutes
    def calculate_time(self, time_column):
        try:
            trim_column = time_column.replace("PT", "")
            hours = trim_column.split("H")
            minutes = 0
            if len(hours) == 2 and hours[1] != "":
                minutes = int(hours[1].split("M")[0]) + int(hours[0]) * 60
            elif len(hours) == 2 and hours[1] == "":
                minutes = int(hours[0]) * 60
            elif len(hours) == 1:
                minutes = int(int(hours[0].split("M")[0]))
            return minutes
        except:
            return 0

    # UDF to calculate complexity of recipes
    def calculate_complexity(self, total_time):
        try:
            if total_time == "" or int(total_time) == 0:
                return "Invalid Case"
            else:
                if int(total_time) < 30:
                    return "easy"
                elif int(total_time) >= 30 and int(total_time) <= 60:
                    return "medium"
                elif int(total_time) > 60:
                    return "difficult"
        except:
            return "Invalid Case"



