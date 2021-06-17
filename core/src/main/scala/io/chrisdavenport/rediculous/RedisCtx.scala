package io.chrisdavenport.rediculous

import cats.data.NonEmptyList
import cats.effect.Async
import scala.annotation.implicitNotFound

/**
  * RedisCtx is the Context in Which RedisOperations operate.
  */
@implicitNotFound("""Cannot find implicit value for  RedisCtx[${F}]. 
If you are trying to build a Redis[F, *], make sure a Async[F] is in scope,
other instances are also present such as RedisTransaction.
If you are leveraging a custom context not provided by rediculous,
please consult your library documentation.
""")
trait RedisCtx[F[_]]{
  def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): F[A]
  def unkeyed[A: RedisResult](command: NonEmptyList[String]): F[A]
}

object RedisCtx {
  
  def apply[F[_]](implicit ev: RedisCtx[F]): ev.type = ev

  implicit def redis[F[_]: Async]: RedisCtx[Redis[F, *]] = new RedisCtx[Redis[F, *]]{
    def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): Redis[F,A] = 
      RedisConnection.runRequestTotal(command, Some(key))
    def unkeyed[A: RedisResult](command: NonEmptyList[String]): Redis[F, A] = 
      RedisConnection.runRequestTotal(command, None)
  }
}