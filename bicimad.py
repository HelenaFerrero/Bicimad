from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, round, sum, col, countDistinct
import matplotlib.pyplot as plt

def anadir_datos(df, rutaFich):
    df1 = spark.read.json(rutaFich)
    df_union = df.union(df1)
    return df_union

def anadir_mes(df):
    # Añadir columna 'month' y eliminar la columna 'unplug_hourTime'
    df_ampliado = df.withColumn("month", month(df.unplug_hourTime.getItem("$date")))\
            .drop('unplug_hourTime')
    return df_ampliado

# Uso de bicicletas por edad
def crear_estadistica_1(df, axes):
    # Gráfica 1
    df_est = df.groupBy('ageRange').count().sort('ageRange')
    dfp = df_est.toPandas().dropna()
    dfp['ageRange'] = dfp['ageRange'].replace([0,1,2,3,4,5,6], ['Rango indeterminado', 'Entre 0 y 16 años', 'Entre 17 y 18 años', 'Entre 19 y 26 años', 'Entre 27 y 40 años', 'Entre 41 y 65 años', 'Entre 66 años o más'])

    ax = axes[0][0]
    dfp.plot.barh(
        ax=ax,
        legend=None,
        x = "ageRange", 
        title = f"Uso de bicicletas por edad",
        xlabel="Cantidad de usuarios",
        ylabel="Rangos de edad",
        rot=0
        )
    ax.bar_label(ax.containers[0])

    # Gráfica 2
    df1 = df.toPandas()
    df1['ageRange'] = df1['ageRange'].replace([0,1,2,3,4,5,6], ['Rango indeterminado', 'Entre 0 y 16 años', 'Entre 17 y 18 años', 'Entre 19 y 26 años', 'Entre 27 y 40 años', 'Entre 41 y 65 años', 'Entre 66 años o más'])
    dfp = df1.groupby('ageRange').count().dropna()

    ax = axes[0][1]
    dfp.plot(kind='pie', y='_id', ylabel="", ax=ax, legend=None, autopct='%.2f%%', title = f"Uso de bicicletas por edad")

# Tiempo de uso de bicicletas por mes
def crear_estadistica_2(df, axes):
    # Gráfica 1
    df_est = df.groupby('month').agg(round(sum('travel_time') / 3600, 0).alias('tiempo en horas')).sort('month').dropna()
    dfp = df_est.toPandas().dropna()
    dfp['month'] = dfp['month'].replace([1,2,3,4,5,6,7,8,9,10,11,12], ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'])

    ax = axes[1][0]
    dfp.plot.bar(
            ax=ax,
            legend=None,
            x = "month", 
            title = f"Tiempo de uso de bicicletas por mes",
            xlabel="Meses",
            ylabel="Tiempo en horas",
            rot=60
        )
    ax.bar_label(ax.containers[0])

    # Gráfica 2
    ax = axes[1][1]
    dfp.plot.line(
            ax=ax,
            legend=None,
            x = "month", 
            title = f"Tiempo de uso de bicicletas por mes",
            xlabel="Meses",
            ylabel="Tiempo en horas",
            rot=60
        )

# Estaciones con mayor uso de bicicletas
def crear_estadistica_3(df, axes):
    # Gráfica 1
    df_est = df.groupBy('idunplug_station').count().sort(col("count").desc()).dropna()
    dfp = df_est.toPandas().head(10)
    
    ax = axes[2][0]
    dfp.plot.bar(
            ax=ax,
            legend=None,
            x = "idunplug_station", 
            title = f"Estaciones con mayor uso de bicicletas",
            xlabel="Nº Estación",
            ylabel="Usos",
            rot=0
        )
    ax.bar_label(ax.containers[0])

    # Gráfica 2
    df1 = df.toPandas()
    dfp = df1.groupby('idunplug_station').count().dropna().head(8)

    ax = axes[2][1]
    dfp.plot(kind='pie', y='_id', ylabel="", ax=ax, legend=None, autopct='%.2f%%', title = f"Estaciones con mayor uso de bicicletas")

# Cantidad de usuarios por mes
def crear_estadistica_4(df, axes):
    # Gráfica 1
    df_est = df.groupBy('month').agg(countDistinct("user_day_code")).sort(col("month").asc()).dropna()
    dfp = df_est.toPandas()
    dfp['month'] = dfp['month'].replace([1,2,3,4,5,6,7,8,9,10,11,12], ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'])

    ax = axes1[0][0]
    dfp.plot.bar(
            ax=ax,
            legend=None,
            x = "month", 
            title = f"Cantidad de usuarios por mes",
            xlabel="Meses",
            ylabel="Usuarios",
            rot=60
        )
    ax.bar_label(ax.containers[0])

    # Gráfica 2
    ax = axes1[0][1]
    dfp.plot.line(
            ax=ax,
            legend=None,
            x = "month", 
            title = f"Cantidad de usuarios por mes",
            xlabel="Meses",
            ylabel="Usuarios",
            rot=60
        )

# Cantidad de movimientos por mes
def crear_estadistica_5(df, axes):
    # Gráfica 1
    df_est = df.groupBy('month').count().sort(col("month").asc()).dropna()
    dfp = df_est.toPandas()
    dfp['month'] = dfp['month'].replace([1,2,3,4,5,6,7,8,9,10,11,12], ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'])

    ax = axes1[1][0]
    dfp.plot.bar(
            ax=ax,
            legend=None,
            x = "month", 
            title = f"Cantidad de movimientos por mes",
            xlabel="Meses",
            ylabel="Movimientos",
            rot=60
        )
    ax.bar_label(ax.containers[0])

    # Gráfica 2
    ax = axes1[1][1]
    dfp.plot.line(
            ax=ax,
            legend=None,
            x = "month", 
            title = f"Cantidad de movimientos por mes",
            xlabel="Meses",
            ylabel="Movimientos",
            rot=60
        )

def mostrar_estadisticas(fig):
    fig.suptitle('De abril 2017 hasta marzo 2018') 
    fig.tight_layout() # separador de gráficos entre columnas y filas
    fig.axes

    fig1.suptitle('De abril 2017 hasta marzo 2018') 
    fig1.tight_layout()
    fig1.axes

    plt.show()

if __name__ == "__main__":
    # Configuraciones iniciales
    spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "15g").getOrCreate()
    fig, axes = plt.subplots(nrows = 3, ncols = 2, squeeze = False, figsize = (10,10)) 
    fig1, axes1 = plt.subplots(nrows = 2, ncols = 2, squeeze = False, figsize = (10,10)) 

    # Crear dataframe con los datos con esquema 1 (con columnas: "_corrupt_record","track")
    df1 = spark.read.json('./data/201704_Usage_Bicimad.json')
    df1 = anadir_datos(df1, './data/201705_Usage_Bicimad.json')
    df1 = anadir_datos(df1, './data/201706_Usage_Bicimad.json')
    df1 = anadir_datos(df1, './data/201707_Usage_Bicimad.json')
    df1 = anadir_datos(df1, './data/201708_Usage_Bicimad.json')
    df1 = anadir_datos(df1, './data/201709_Usage_Bicimad.json')
    df1 = anadir_datos(df1, './data/201710_Usage_Bicimad.json')
    df = df1.drop("_corrupt_record","track")

    # Crear dataframe con los datos con esquema 2 (sin columnas: "_corrupt_record","track")
    df2 = spark.read.json('./data/201711_Usage_Bicimad.json')
    df2 = anadir_datos(df2, './data/201712_Usage_Bicimad.json')
    df2 = anadir_datos(df2, './data/201801_Usage_Bicimad.json')
    df2 = anadir_datos(df2, './data/201802_Usage_Bicimad.json')
    df2 = anadir_datos(df2, './data/201803_Usage_Bicimad.json')
    
    df = df.union(df2)

    df = anadir_mes(df)
    df_muestra = df.sample(0.02)
    df = df_muestra
    # Crear estadísticas
    crear_estadistica_1(df, axes)
    crear_estadistica_2(df, axes)
    crear_estadistica_3(df, axes)
    crear_estadistica_4(df, axes)
    crear_estadistica_5(df, axes)

    mostrar_estadisticas(fig)