package oiosmdiff

import java.net.URI

import com.typesafe.scalalogging.LazyLogging

import geotrellis.proj4.{WebMercator, LatLng}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{Geometry, Feature}
import geotrellis.vectortile.{VString, VectorTile}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import org.locationtech.geomesa.spark.jts._

import vectorpipe._
import vectorpipe.GenerateVT.VTF
import vectorpipe.functions.osm._


class OiOsmDiff(
  osmOrcUri: URI,
  oiGeoJsonUri: URI
)(@transient implicit val ss: SparkSession) extends LazyLogging with Serializable {
  def saveTilesForZoom(zoom: Int, outputS3Prefix: URI): Unit = {
    val badRoads = Seq("proposed", "construction", "elevator")

    val osmData = OSM.toGeometry(ss.read.orc(osmOrcUri.toString))

    val osmRoadData = osmData
    // does isRoad check to see if the geometry is valid?
      .filter(isRoad(col("tags")))
      .withColumn("roadType", osmData("tags").getField("highway"))
      .withColumn("surfaceType", osmData("tags").getField("surface"))

    val osmRoads = osmRoadData.where(!osmRoadData("roadType").isin(badRoads:_*))

    val osmRoadsRDD: RDD[VTF[Geometry]] =
      osmRoads
        .rdd
        .map { row =>
          val id          = row.getAs[String]("id")

          val roadType    =
            row.getAs[String]("roadType") match {
              case null => "null"
              case s: String => s
            }

          val surfaceType =
            row.getAs[String]("surfaceType") match {
              case null => "null"
              case s: String => s
            }

          val geom        = row.getAs[JTSGeometry]("geom")
          val reprojectedGeom = Geometry(geom).reproject(LatLng, WebMercator)

          val featureInfo: Map[String, VString] =
            Map(
              "id"          -> VString(id),
              "roadType"    -> VString(roadType),
              "surfaceType" -> VString(surfaceType)
            )

          Feature(reprojectedGeom, featureInfo)
        }

    val layoutScheme     = ZoomedLayoutScheme(WebMercator)
    val layout           = layoutScheme.levelForZoom(zoom).layout
    val keyedOsmRoadsRDD: RDD[(SpatialKey, (SpatialKey, VTF[Geometry]))] = GenerateVT.keyToLayout(osmRoadsRDD, layout)

    val bucket      = outputS3Prefix.getHost
    val path        = outputS3Prefix.getPath.stripPrefix("/")
    val vectorTiles: RDD[(SpatialKey, VectorTile)] = GenerateVT.makeVectorTiles(keyedOsmRoadsRDD, layout, "osm-roads")
    GenerateVT.save(vectorTiles, zoom, bucket, path)
  }
}
