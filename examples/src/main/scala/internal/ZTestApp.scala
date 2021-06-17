import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.data._
import cats.effect._
import fs2.io.tcp._
import io.chrisdavenport.rediculous.cluster.ClusterCommands
import cats.effect.Resource

/**
 * Test App For Development Purposes
 **/
object ZTestApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Resource.unit[IO]
      sg <- SocketGroup[IO](blocker)
      connection <- RedisConnection.pool[IO](sg, "localhost", 30001)
    } yield connection

    r.use {con =>
      RedisConnection.runRequestTotal[IO, ClusterCommands.ClusterSlots](NonEmptyList.of("CLUSTER", "SLOTS"), None).unRedis.run(con).flatten
      .flatTap(r => IO(println(r)))
    } >>
      IO.pure(ExitCode.Success)
    
  }

}