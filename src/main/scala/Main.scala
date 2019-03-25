package oiosmdiff

import java.net.URI

import geotrellis.spark.io.kryo.KryoRegistrator
import cats.implicits._
import com.monovore.decline._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import scala.util.Properties

object Main
    extends CommandApp(
      name = "oi-osm-diff",
      header = "Diffs OI derived road data with OSM",
      main = {
        val osmOrcUriOpt =
          Opts
            .argument[URI]("osmOrcUri")
            .validate("osmOrcUri must be an S3 Uri") { _.getScheme.startsWith("s3") }
            .validate("osmOrcUri must be an .orc file") { _.getPath.endsWith(".orc") }
        val oiGeoJsonUriOpt =
          Opts
            .argument[URI]("oiGeoJsonUri")
            .validate("oiGeoJsonUri must be an S3 Uri") { _.getScheme.startsWith("s3") }
            .validate("oiGeoJsonUri must be a .geojson file") { _.getPath.endsWith(".geojson") }
        val outputS3PrefixOpt =
          Opts
            .argument[URI]("outputS3PathPrefix")
            .validate("outputS3PathPrefix must be an S3 Uri") { _.getScheme.startsWith("s3") }

        (osmOrcUriOpt, oiGeoJsonUriOpt, outputS3PrefixOpt).mapN {
          (osmOrcUri, oiGeoJsonUri, outputS3Prefix) =>
            val conf =
              new SparkConf()
                .setIfMissing("spark.master", "local[*]")
                .set("spark.serializer", classOf[KryoSerializer].getName)
                .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
                .set("spark.executionEnv.AWS_PROFILE",
                     Properties.envOrElse("AWS_PROFILE", "default"))

            implicit val spark = SparkSession.builder
              .appName("OI OSM Diff")
              .enableHiveSupport
              .config(conf)
              .getOrCreate

            try {
              val vectorDiff = new OiOsmDiff(osmOrcUri, oiGeoJsonUri)
              vectorDiff.saveTilesForZoom(12, outputS3Prefix)
            } catch {
              case e: Exception => throw e
            } finally {
              spark.stop
            }
        }
      }
    )
