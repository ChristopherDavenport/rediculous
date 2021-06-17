package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.data._
import cats.effect._

/**
 * For When you don't trust automatic pipelining. 
 * 
 * ClusterMode: Multi Key Operations Will use for the first key
 * provided.
 * 
 * [[pipeline]] method converts the Pipeline state to the Redis Monad. This will error
 * if you pipeline and have not actually enterred any redis commands.
 **/
final case class RedisPipeline[A](value: RedisTransaction.RedisTxState[RedisTransaction.Queued[A]]){
  def pipeline[F[_]: Async]: Redis[F, A] = RedisPipeline.pipeline[F](this)
}

object RedisPipeline {

  implicit val ctx: RedisCtx[RedisPipeline] =  new RedisCtx[RedisPipeline]{
    def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): RedisPipeline[A] = 
      RedisPipeline(RedisTransaction.RedisTxState{for {
        s1 <- State.get[(Int, List[NonEmptyList[String]], Option[String])]
        (i, base, value) = s1
        _ <- State.set((i + 1, command :: base, value.orElse(Some(key))))
      } yield RedisTransaction.Queued(l => RedisResult[A].decode(l(i)))})

    def unkeyed[A: RedisResult](command: NonEmptyList[String]): RedisPipeline[A] = RedisPipeline(RedisTransaction.RedisTxState{for {
      out <- State.get[(Int, List[NonEmptyList[String]], Option[String])]
      (i, base, value) = out
      _ <- State.set((i + 1, command :: base, value))
    } yield RedisTransaction.Queued(l => RedisResult[A].decode(l(i)))})
  }

  implicit val applicative: Applicative[RedisPipeline] = new Applicative[RedisPipeline]{
    def pure[A](a: A) = RedisPipeline(Monad[RedisTransaction.RedisTxState].pure(Monad[RedisTransaction.Queued].pure(a)))

    override def ap[A, B](ff: RedisPipeline[A => B])(fa: RedisPipeline[A]): RedisPipeline[B] =
      RedisPipeline(RedisTransaction.RedisTxState(
        Nested(ff.value.value).ap(Nested(fa.value.value)).value
      ))
  }

  val fromTransaction = new (RedisTransaction ~> RedisPipeline){
    def apply[A](fa: RedisTransaction[A]): RedisPipeline[A] = RedisPipeline(fa.value)
  }

  val toTransaction = new (RedisPipeline ~> RedisTransaction){
    def apply[A](fa: RedisPipeline[A]): RedisTransaction[A] = RedisTransaction(fa.value)
  }

  def pipeline[F[_]] = new SendPipelinePartiallyApplied[F]


  class SendPipelinePartiallyApplied[F[_]]{
    def apply[A](tx: RedisPipeline[A])(implicit F: Async[F]): Redis[F, A] = {
      Redis(Kleisli{(c: RedisConnection[F]) => 
        val ((_, commandsR, key), RedisTransaction.Queued(f)) = tx.value.value.run((0, List.empty, None)).value
        val commands = commandsR.reverse.toNel
        commands.traverse(nelCommands => RedisConnection.runRequestInternal(c)(nelCommands, key) // We Have to Actually Send A Command
          .map{fNel => RedisConnection.closeReturn(fNel.map(a => f(a.toList)))}
        ).flatMap{fOpt => 
          fOpt.map(_.pure[F]).getOrElse(F.raiseError(RedisError.Generic("Rediculous: Attempted to Pipeline Empty Command")))
        }
      })
    }
  }

}