package oiosmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{MultiLine, MultiLineFeature}
import geotrellis.vectortile.VString
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.locationtech.geomesa.spark.jts._
import vectorpipe._
import vectorpipe.GenerateVT.VTF
import vectorpipe.functions.osm._

class OiOsmDiff(osmOrcUri: URI, oiGeoJsonUri: URI)(@transient implicit val spark: SparkSession)
    extends LazyLogging
    with Serializable {

  def saveTilesForZoom(zoom: Int, outputS3Prefix: URI): Unit = {

    spark.withJTS

    val badRoads = Seq("proposed", "construction", "elevator")

    val osmData = OSM.toGeometry(spark.read.orc(osmOrcUri.toString))
    val osmRoadData = osmData
      .filter(isRoad(col("tags")))
      .withColumn("roadType", osmData("tags").getField("highway"))
      .withColumn("surfaceType", osmData("tags").getField("surface"))
    val osmRoads = osmRoadData.where(!osmRoadData("roadType").isin(badRoads))

    val osmRoadsRdd: RDD[VTF[MultiLine]] = osmRoads.rdd.map(row => {
      val id          = row.getAs[String]("id")
      val geom        = row.getAs[MultiLine]("geom")
      val roadType    = row.getAs[String]("roadType")
      val surfaceType = row.getAs[String]("surfaceType")

      MultiLineFeature(geom,
                       Map("id"          -> VString(id),
                           "roadType"    -> VString(roadType),
                           "surfaceType" -> VString(surfaceType)))
    })

    val layoutScheme     = ZoomedLayoutScheme(WebMercator)
    val layout           = layoutScheme.levelForZoom(zoom).layout
    val keyedOsmRoadsRdd = GenerateVT.keyToLayout(osmRoadsRdd, layout)

    val bucket      = outputS3Prefix.getHost
    val path        = outputS3Prefix.getPath.stripPrefix("/")
    val vectorTiles = GenerateVT.makeVectorTiles(keyedOsmRoadsRdd, layout, "osm-roads")
    GenerateVT.save(vectorTiles, zoom, bucket, path)
  }
}
