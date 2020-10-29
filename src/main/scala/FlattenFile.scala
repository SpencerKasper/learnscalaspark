import columns.{Accounts, Purchases}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, desc, explode, sum}

object FlattenFile {

  def main(args: Array[String]): Unit = {
    val spark = new Spark("flattenFilePractice")
      .get()

    val idSourceColumn = col("id")
    val innerListSourceColumn = col("innerList")

    val nonFlatDataFrame = spark.read.parquet("parquet\\non-flat-file.parquet")

    val flattenedItemColumn = explode(innerListSourceColumn)
      .as("itemFromInnerList")
    val spreadItemFromInnerList = col("itemFromInnerList.*")

    nonFlatDataFrame
      .select(idSourceColumn, flattenedItemColumn)
      .select(idSourceColumn, spreadItemFromInnerList)
      .write.parquet("parquet\\flat-file.parquet")
  }
}