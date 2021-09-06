package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = agregaAgeRange(df);
        df = agregaRankByNationality(df);
        df = agregaPotentialVsOverall(df);
        df = filterData(df);

        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        //df.show(100, false);
        df.show();
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column());
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {

        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df is a Dataset with players information
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {

        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have age column)
     * @return add to the Dataset the column "age_range"
     * by each position value
     * cat A for if age is less 23
     * cat B for if age is less 27
     * cat C for if age is less 32
     * cat D for the rest
     */
    private Dataset<Row> agregaAgeRange(Dataset<Row> df) {

        df = df.withColumn(ageRange.getName(), when(col("age").$less(23), "A")
                .when(col("age").$less(27), "B")
                .when(col("age").$less(32), "C")
                .otherwise("D"));
        return df;
    }

    /**
     * @param df is a Dataset with players information (must have nationality and teamPosition columns)
     * @return add to the Dataset the column "rank_by_nationality_position"
     * order by overall column with row_number
     */
    private Dataset<Row> agregaRankByNationality(Dataset<Row> df) {

        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rowNumber = row_number().over(w);

        df = df.withColumn(rankByNationalityPosition.getName(), rowNumber);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have potential and overall columns)
     * @return add to the Dataset the column "potential_vs_overall"
     *
     */
    private Dataset<Row> agregaPotentialVsOverall(Dataset<Row> df) {

        df = df.withColumn(potentialVsOverall.getName(), col("potential").divide(col("overall")));
        return df;
    }

    /**
     * @param df is a Dataset with players information (must have potential and overall columns)
     * @return filter Dataset with the given conditions
     * Si `rank_by_nationality_position` es menor a **3**
     * Si `age_range` es **B** o **C** y `potential_vs_overall` es superior a **1.15**
     * Si `age_range` es **A** y `potential_vs_overall` es superior a **1.25**
     * Si `age_range` es **D** y `rank_by_nationality_position` es menor a **5**
     */
    private Dataset<Row> filterData(Dataset<Row> df) {

        df = df.filter(rankByNationalityPosition.column().$less(3));
        df = df.filter(ageRange.column().isin("B", "C").and(potentialVsOverall.column().$greater(1.15)));
        df = df.filter(ageRange.column().equalTo("A").and(potentialVsOverall.column().$greater(1.25)));
        df = df.filter(ageRange.column().equalTo("D").and(rankByNationalityPosition.column().$less(5)));
        return df;
    }



}
