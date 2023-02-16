# Databricks notebook source
# MAGIC %md # Bare Soil Survey ETL
# MAGIC
# MAGIC We have received some survey data to evaluate our model
# MAGIC for the bare soils analysis. This notebook will move the
# MAGIC data sets uploaded to File Store to the correct location
# MAGIC before analysis occurs.
# MAGIC
# MAGIC __Prerequisites__:
# MAGIC - CSV fie must be uploaded to /dbfs/FileStore/ manually
# MAGIC - You must know the file name and change the file_name variable in
# MAGIC the code (line 5)
# MAGIC
# MAGIC __What should happen__:
# MAGIC - the file from FleStore should be moved to the survey_data folder
# MAGIC in the elm/elmo/bare_soil area.
# MAGIC - the output should show you a list of files in the survey__data
# MAGIC folder which should include the new file.

# COMMAND ----------

# MAGIC %sh
# MAGIC function move_survey_data () {
# MAGIC     local from_path='/dbfs/FileStore/'
# MAGIC     local to_path='/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/survey_data/'
# MAGIC     local file_name='SFI_ArableAndHorticulturalSoil_Autumn_Quadrat.csv'
# MAGIC
# MAGIC     cd /dbfs/FileStore
# MAGIC     mv $file_name /dbfs/
# MAGIC     cd ..
# MAGIC     mv $file_name /
# MAGIC     cd ..
# MAGIC     mv $file_name $to_path
# MAGIC     echo We have now moved now moved $file_path to $to_path
# MAGIC     cd $to_path
# MAGIC     echo $file_name has been moved to $to_path
# MAGIC     echo The following files are in $to_path :
# MAGIC     ls
# MAGIC }
# MAGIC move_survey_data
