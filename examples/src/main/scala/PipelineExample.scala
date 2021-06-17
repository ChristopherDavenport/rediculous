import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.io.tcp._
import cats.effect.Resource

// Send a Single Set of Pipelined Commands to the Redis Server
object PipelineExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Resource.unit[IO]
      sg <- SocketGroup[IO](blocker)
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 10000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO](sg, "localhost", 6379, maxQueued = 10000, workers = 2)
    } yield connection

    r.use {client =>
      val r = (
        RedisCommands.ping[RedisPipeline],
        RedisCommands.del[RedisPipeline]("foo"),
        RedisCommands.get[RedisPipeline]("foo"),
        RedisCommands.set[RedisPipeline]("foo", "value"),
        RedisCommands.get[RedisPipeline]("foo")
      ).tupled

      val multi = r.pipeline[IO]

      multi.run(client).flatTap(output => IO(println(output)))

    }.as(ExitCode.Success)

  }
}