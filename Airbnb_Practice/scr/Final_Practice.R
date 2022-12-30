#'**Práctica Final Spark - Extracción, Transformación y Carga (ETL). Víctor Viloria Vázquez**
#'
#' Antes de cargar el script, se debe modificar los directorios relativos de los procesos de carga
#' y exportación de los archivos .parquet. Dado que no se puede adjuntar un .rar con las carpetas en CANVAS. 
#'
#'**LIBRERIAS. Cargamos las librerias que necesesitaremos para la realización de la práctica.**
#'
suppressWarnings({
  
# Use Spark in R.
  
library(sparklyr) 

# Manipulation of data.
  
library(dplyr)
  
# Save as parquet files.

library(arrow)

})

# Set a seed for the future sample transformations.

set.seed(42)


#'**Fase 1: Extracción**
#'
#'
#'1. Extracción (listings) Crea un data frame de pandas o de R a partir del fichero
#'listings_redux.parquet con las consideraciones que se indican a continuación. Con
#'Spark, haz un join con el fichero neighbourhoods.parquet para añadir el dato de distrito
#'(neighbourhood_group) y asegúrate de que extraes esta columna en el data frame
#'en lugar de neighbourhood. Para saber qué columnas necesitas, tendrás que seguir
#'leyendo la práctica para ver qué es lo que irás necesitando.
#'

# Install the spark server.

#spark_install()


# Execute the spark server as local.

sc <- spark_connect(master = "local")

# Save the listings parquet as 'listings_spark' in the local server.

listings_spark <- spark_read_parquet(sc, 
                                   name = "listings", 
                                   path = "../data/raw/listings_redux.parquet")


# Save the neighbourhoods parquet as 'neighbourhoods_spark' in the local server.



neighbourhoods_spark <- spark_read_parquet(sc, 
                                     name = "neighbourhoods", 
                                     path = "../data/raw/neighbourhoods.parquet")


# Join both DF by neighbourhood in  'listing_districts'.



listing_districts <- inner_join(listings_spark,
                                neighbourhoods_spark,
                                by = c("neighbourhood_cleansed" = "neighbourhood"))
                      


# Select the useful variables for the practice and then save the spark DF as an r DF called 'listings_df'.

listings_df <-  listing_districts %>% 
  select(id, price,room_type,neighbourhood_group, number_of_reviews,review_scores_rating) %>% 
  collect()


# Check the values and variables of 'listings_df'.

glimpse(listings_df)


#' Extracción (reviews) Trabaja en Spark la información del fichero reviews.parquet
#'con las consideraciones siguientes. Crea un data frame de R o pandas con la tabla final.
#'• Con Spark, haz un join con la información del fichero neighbourhoods.parquet
#'para añadir el dato de distrito (neighbourhood_group) y asegúrate de que extraes
#'esta columna en el data frame en lugar de neighbourhood.
#'• También en Spark, cuenta a nivel de distrito y mes el número de reviews. Para
#'calcular el mes a partir de una fecha te vendrá bien la documentación oficial de
#'pyspark y el método strftime (en este link) o esta respuesta en StackOverflow sobre
#'sparklyr.
#'• Además, extrae los datos desde 2011 en adelante (también con Spark).


# Save the reviews parquet as 'reviews_spark' in the local server.


reviews_spark <- spark_read_parquet(sc, 
                                     name = "reviews", 
                                     path = "../data/raw/reviews.parquet")



# Join 'listing_districts' and 'reviews_spark' dataframes by the ID of the listings in 'review_districts'.


review_districts <- inner_join(listing_districts,
                               reviews_spark,
                               by = c("id" = "listing_id"))



# Convert dates from daily to monthly.

review_districts <- review_districts %>%  
  mutate(date=from_unixtime(
    unix_timestamp(date, "yyyy-MM-dd"), "yyyy-MM"))


# Group by dates, districts with the sum of reviews in an R dataframe called 'reviews_df'.

reviews_df <- review_districts %>%
  filter(date > '2010-12-31') %>% 
  group_by(date,neighbourhood_group) %>% 
  summarise(Unique_Reviews = n_distinct(id_y), .groups = 'drop') %>% 
  arrange(date) %>% 
  collect()
  
  
# Check the values and variables of 'reviews_df'.

glimpse(reviews_df)


#'**Fase 2: Transformación**
#'
#'3. Transformación (listings). Antes de realizar la agregación que se pide, tienes que
#'tratar las columnas price,, number_of_reviews y review_scores_rating. Empieza
#'con el precio (de las otras dos columnas te encargarás en el siguiente ejercicio). Necesitas
#'pasarla a numérica. Ahora mismo es de tipo texto y lo primero que necesitamos es quitar
#'símbolos raros.


# Transform column price without the $ symbol and replacing the ',' for '.'

listings_df <- listings_df %>% 
  mutate(price = gsub("\\$|,", "", price),
         price = as.numeric(gsub(",", ".", price))
  )


# Check the transformation done in 'listings_df.

glimpse(listings_df)



#' 4. Transformación (listings). Toca imputar los valores missing de number_of_reviews
#'y review_scores_rating. Normalmente en estos casos se habla con la gente que más
#'usa los datos y se llega con ellos a un acuerdo de cómo se imputaría esta información.
#'En este caso, imputa los valores missing con valores reales dentro de la tabla,
#'a nivel de room_type, escogidos de manera aleatoria. Es decir, si hay un valor missing
#'en number_of_reviews para un registro con room_type == "Entire home/apt", lo
#'reemplazarías con un valor aleatorio de esa misma columna para los que room_type sea
#'"Entire home/apt". Tienes libertad para plantear esto como te resulte más cómodo.
#'Pista. Yo he hecho un bucle for() con R base (sí, lo nunca visto en mí :P)
#'

# Group by 'room_type' to generate in 'number_of_reviews' and 'review_scores_rating' random samples
# replacing the NaN values in  order to 'room_type'.

listings_df <- listings_df %>%
  group_by(room_type) %>%
  mutate(across(c(number_of_reviews, review_scores_rating), ~ coalesce(., sample(na.omit(.), size=n(), replace=TRUE)))) %>%
  ungroup()


# Check the transformation done in 'listings_df'.

glimpse(listings_df)


#' 5. Transformación (listings). Con los missing imputados y el precio en formato numérico
#' ya puedes agregar los datos. A nivel de distrito y de tipo de alojamiento, hay que calcular:
#'   • Nota media ponderada (review_scores_rating ponderado con number_of_reviews).
#'   • Precio mediano (price).
#'   • Número de alojamientos (id).


# Group by district and room_type, then add two new columns with the median of the price
# ,the weighted mean as the statement specifies and the number of houses.

listings_df <- listings_df %>%
  group_by(neighbourhood_group, room_type) %>% 
  summarise(nota_media = weighted.mean(review_scores_rating, number_of_reviews),
            precio_mediano = median(price),
            IDs = n_distinct(id),
            .groups = 'drop')

# Rename the different variables as the statement specifies.

names(listings_df) = c("distrito", 
                       "tipo_de_alojamiento",
                       "nota_media",
                       "precio_mediano",
                       "Nº_alojamientos")

# Check the transformation done in 'listings_df'.

head(listings_df)

#'6. Transformación (reviews). La mayor parte de la transformación para Reviews la has
#'hecho ya con SQL. Vamos a añadir ahora a simular que tenemos un modelo predictivo
#'y lo vamos a aplicar sobre nuestros datos. Así, la tabla que subamos de nuevo a la
#'base de datos tendrá la predicción añadida. El último mes disponible es julio, así que
#'daremos la predicción para agosto. Esto no es una asignatura de predicción de series
#'temporales, así que nos vamos a conformar con tomar el valor de julio como predicción
#'para agosto (a nivel de distrito). Es decir, si el dato en "Centro" para julio es de 888
#'reviews, añadiremos una fila con los valores "Centro", "2021-08" y 888, así para cada
#'distrito. Tienes libertad para plantearlo como veas adecuado. Al final, deja el data
#'frame ordenado a nivel de distrito y mes. 



# Select the data of July 2021 and extract it in 'reviews_august' changing the date to August 2021.

reviews_august<- reviews_df %>%
  filter(date =='2021-07') %>% 
  mutate(date = "2021-08")
  


# Append the new rows created of august into 'reviews_df'

reviews_df<- bind_rows(reviews_df,
                       reviews_august)


# Translate the variable names of 'reviews_df' into spanish.

names(reviews_df) = c("fecha", 
                       "distrito",
                       "reviews_unicas"
                      )

# Check the transformation done in 'reviews_df'.

head(reviews_df)

#' 7. Transformación (reviews). Hay casos que no tienen dato, por ejemplo,
#'febrero de 2011 en Arganzuela. Como no hay dato, asumiremos que es 0. Siguiendo
#'esta idea, añade todos los registros necesarios a la tabla. Puedes hacerlo de la manera
#'que te resulte más intuitiva. Recuerda ordenar la tabla final por distrito y mes.


# Save the distinct dates and districts in individual variables.

distinct_dates <- reviews_df %>% 
  distinct(fecha) %>%
  unlist()

distinct_districts <- reviews_df %>% 
  distinct(distrito) %>%
  unlist()


# Create a dataframe 'df' with all the districts each month.

df <- data.frame(fecha = rep(distinct_dates, each = length(distinct_districts)),
                 distrito = distinct_districts,row.names = NULL)


# Full join between 'df' and 'reviews_df' in a new variable called 'full_reviews'.
# Then, order by district and date.

full_reviews <- merge(x = df, 
                      y = reviews_df,
                      all = TRUE) %>%
  mutate(reviews_unicas = ifelse(is.na(reviews_unicas), 0, reviews_unicas)) %>% 
  arrange(distrito, fecha)


# Check the transformation done in 'full_reviews'.

head(full_reviews)
                

#'**Fase 3: Carga**
#'
#' 8. Carga. Guarda en formato parquet las dos tablas que has creado. No sobreescibas
#' los que tienes: crea dos ficheros nuevos


# Save the final dataframes 'full_reviews', 'listings_df' as a parquet file.

write_parquet(full_reviews, "../data/processed/full_reviews_final.parquet")
write_parquet(listings_df, "../data/processed/listing_df_final.parquet")

# Test to check the new files created.

test_reviews <- read_parquet("../data/processed/full_reviews_final.parquet")
test_listings <- read_parquet("../data/processed/listing_df_final.parquet")

# Display the files loaded.

head(test_reviews)
head(test_listings)

#Disconnect the spark server.

spark_disconnect(sc)

