package io.chrisdavenport.rediculous

import cats.data.NonEmptyList
import cats.effect.Concurrent
import scala.annotation.implicitNotFound

/**
  * RedisCtx is the Context in Which RedisOperations operate.
  */
@implicitNotFound("""Cannot find implicit value for  RedisCtx[${F}]. 
If you are trying to build a Redis[F, *], make sure a Concurrent[F] is in scope,
other instances are also present such as RedisTransaction.
If you are leveraging a custom context not provided by rediculous,
please consult your library documentation.
""")
trait RedisCtx[F[_]]{
  def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): F[A]
  def unkeyed[A: RedisResult](command: NonEmptyList[String]): F[A]
  def broadcast[A: RedisResult](command: NonEmptyList[String]): F[A]
}

object RedisCtx {
  
  def apply[F[_]](implicit ev: RedisCtx[F]): ev.type = ev

  sealed trait CtxType
  object CtxType {
    case class Keyed(key: String) extends CtxType
    case object Broadcast extends CtxType
    case object Random extends CtxType
  }

  implicit def redis[F[_]: Concurrent]: RedisCtx[Redis[F, *]] = new RedisCtx[Redis[F, *]]{
    def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): Redis[F,A] = 
      RedisConnection.runRequestTotal(command, CtxType.Keyed(key))
    def unkeyed[A: RedisResult](command: NonEmptyList[String]): Redis[F, A] = 
      RedisConnection.runRequestTotal(command, CtxType.Random)
    def broadcast[A: RedisResult](command: NonEmptyList[String]): Redis[F, A] =
      RedisConnection.runRequestTotal(command, CtxType.Broadcast)
  }
}