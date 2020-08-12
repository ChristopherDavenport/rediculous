package io.chrisdavenport.rediculous

import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import cats._
import cats.implicits._
import cats.data._
import io.chrisdavenport.keypool._
import fs2.concurrent.Queue
import fs2.io.tcp._
import fs2._
import java.net.InetSocketAddress
import scala.concurrent.duration._
import _root_.io.chrisdavenport.rediculous.cluster.HashSlot
import _root_.io.chrisdavenport.rediculous.cluster.ClusterCommands


sealed trait RedisConnection[F[_]]
object RedisConnection{
  private case class Queued[F[_]](queue: Queue[F, Chunk[(Deferred[F, Either[Throwable, Resp]], Resp)]], usePool: Resource[F, Managed[F, Socket[F]]]) extends RedisConnection[F]
  private case class PooledConnection[F[_]](
    pool: KeyPool[F, Unit, (Socket[F], F[Unit])]
  ) extends RedisConnection[F]
  private case class DirectConnection[F[_]](socket: Socket[F]) extends RedisConnection[F]

  private case class Cluster[F[_]](queue: Chunk[(Deferred[F, Either[Throwable, Resp]], Option[String], Resp)] => F[Unit]) extends RedisConnection[F]

  // Guarantees With Socket That Each Call Receives a Response
  // Chunk must be non-empty but to do so incurs a penalty
  private[rediculous] def explicitPipelineRequest[F[_]: MonadError[*[_], Throwable]](socket: Socket[F], calls: Chunk[Resp], maxBytes: Int = 8 * 1024 * 1024, timeout: Option[FiniteDuration] = 5.seconds.some): F[List[Resp]] = {
    def getTillEqualSize(acc: List[List[Resp]], lastArr: Array[Byte]): F[List[Resp]] = 
    socket.read(maxBytes, timeout).flatMap{
      case None => 
        ApplicativeError[F, Throwable].raiseError[List[Resp]](new Throwable("Rediculous: Terminated Before reaching Equal size"))
      case Some(bytes) => 
        Resp.parseAll(lastArr.toArray ++ bytes.toArray.toIterable) match {
          case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[List[Resp]](e)
          case Resp.ParseIncomplete(arr) => getTillEqualSize(acc, arr)
          case Resp.ParseComplete(value, rest) => 
            if (value.size + acc.foldMap(_.size) === calls.size) (value ::acc ).reverse.flatten.pure[F]
            else getTillEqualSize(value :: acc, rest)
          
        }
    }
    if (calls.nonEmpty){
      val arrayB = new scala.collection.mutable.ArrayBuffer[Byte]
        calls.toList.foreach{
          case resp => 
            arrayB.++=(Resp.encode(resp))
        }
      socket.write(Chunk.bytes(arrayB.toArray)) >>
      getTillEqualSize(List.empty, Array.emptyByteArray)
    } else Applicative[F].pure(List.empty)
  }

  def runRequestInternal[F[_]: Concurrent](connection: RedisConnection[F])(
    inputs: NonEmptyList[NonEmptyList[String]],
    key: Option[String]
  ): F[F[NonEmptyList[Resp]]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      def withSocket(socket: Socket[F]): F[NonEmptyList[Resp]] = explicitPipelineRequest[F](socket, chunk).flatMap(l => Sync[F].delay(l.toNel.getOrElse(throw new Throwable("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))))
      connection match {
      case PooledConnection(pool) => pool.map(_._1).take(()).use{
        m => withSocket(m.value).attempt.flatTap{
          case Left(_) => m.canBeReused.set(Reusable.DontReuse)
          case _ => Applicative[F].unit
        }
      }.rethrow.map(_.pure[F])
      case DirectConnection(socket) => withSocket(socket).map(_.pure[F])
      case Queued(queue, _) => chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map((_, resp))).flatMap{ c => 
        queue.enqueue1(c).as {
          c.traverse(_._1.get).flatMap(_.sequence.traverse(l => Sync[F].delay(l.toNel.getOrElse(throw new Throwable("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))))).rethrow
        }   
      }
      case Cluster(queue) => chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map((_, key, resp))).flatMap{ c => 
        queue(c).as {
          c.traverse(_._1.get).flatMap(_.sequence.traverse(l => Sync[F].delay(l.toNel.getOrElse(throw new Throwable("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))))).rethrow
        }
      }   
    }
  }

  // Can Be used to implement any low level protocols.
  def runRequest[F[_]: Concurrent, A: RedisResult](connection: RedisConnection[F])(input: NonEmptyList[String], key: Option[String]): F[F[Either[Resp, A]]] = 
    runRequestInternal(connection)(NonEmptyList.of(input), key).map(_.map(nel => RedisResult[A].decode(nel.head)))

  def runRequestTotal[F[_]: Concurrent, A: RedisResult](input: NonEmptyList[String], key: Option[String]): Redis[F, A] = Redis(Kleisli{connection: RedisConnection[F] => 
    runRequest(connection)(input, key).map{ fE => 
      fE.flatMap{
        case Right(a) => a.pure[F]
        case Left(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError[A](e)
        case Left(other) => ApplicativeError[F, Throwable].raiseError[A](new Throwable(s"Rediculous: Incompatible Return Type for Operation: ${input.head}, got: $other"))
      }
    }
  })

  private[rediculous] def closeReturn[F[_]: MonadError[*[_], Throwable], A](fE: F[Either[Resp, A]]): F[A] = 
    fE.flatMap{
        case Right(a) => a.pure[F]
        case Left(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError[A](e)
        case Left(other) => ApplicativeError[F, Throwable].raiseError[A](new Throwable(s"Rediculous: Incompatible Return Type: Got $other"))
      }

  def single[F[_]: Concurrent: ContextShift](sg: SocketGroup, address: InetSocketAddress): Resource[F, RedisConnection[F]] = 
    sg.client[F](address).map(RedisConnection.DirectConnection(_))

  def pool[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress): Resource[F, RedisConnection[F]] = 
    KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
      {_ => sg.client[F](address).allocated},
      { case (_, shutdown) => shutdown}
    ).build.map(PooledConnection[F](_))

  // Only allows 1k queued actions, before new actions block to be accepted.
  def queued[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress, maxQueued: Int = 1000, workers: Int = 2): Resource[F, RedisConnection[F]] = 
    for {
      queue <- Resource.liftF(Queue.bounded[F, Chunk[(Deferred[F, Either[Throwable,Resp]], Resp)]](maxQueued))
      keypool <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
        {_ => sg.client[F](address).allocated},
        { case (_, shutdown) => shutdown}
      ).build
      _ <- 
          queue.dequeue.chunks.map{chunkChunk =>
            val chunk = chunkChunk.flatten
            val s = if (chunk.nonEmpty) {
                Stream.eval(keypool.map(_._1).take(()).use{m =>
                  val out = chunk.map(_._2)
                  explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                    case Left(_) => m.canBeReused.set(Reusable.DontReuse)
                    case _ => Applicative[F].unit
                  }
                }.flatMap{
                  case Right(n) => 
                    n.zipWithIndex.traverse_{
                      case (ref, i) => 
                        val (toSet, _) = chunk(i)
                        toSet.complete(Either.right(ref))
                    }
                  case e@Left(_) => 
                    chunk.traverse_{ case (deff, _) => deff.complete(e.asInstanceOf[Either[Throwable, Resp]])}
                }) 
            } else {
              Stream.empty
            }
            s ++ Stream.eval_(ContextShift[F].shift)
          }.parJoin(workers) // Worker Threads
          .compile
          .drain
          .background
    } yield Queued(queue, keypool.take(()).map(_.map(_._1)))

  def cluster[F[_]: Concurrent: Parallel: Timer: ContextShift](sg: SocketGroup, host: String, port: Int, maxQueued: Int = 10000): Resource[F, RedisConnection[F]] = 
    for {
      sockets <- Resource.liftF(single(sg, new InetSocketAddress(host, port)).use(ClusterCommands.clusterslots[Redis[F, *]].unRedis.run(_).flatten))
      refTopology <- Resource.liftF(Ref[F].of(sockets))
      queue <- Resource.liftF(Queue.bounded[F, Chunk[(Deferred[F, Either[Throwable,Resp]], Option[String], Resp)]](maxQueued))
      keypool <- KeyPoolBuilder[F, (String, Int), (Socket[F], F[Unit])](
        {t: (String, Int) => sg.client[F](new InetSocketAddress(t._1, t._2)).allocated},
        { case (_, shutdown) => shutdown}
      ).build
      cluster = Cluster(queue.enqueue1)
      refreshTopology = ClusterCommands.clusterslots[Redis[F, *]].unRedis.run(cluster).flatten.flatMap(refTopology.set)
      _ <- 
          queue.dequeue.chunks.map{chunkChunk =>
            val chunk = chunkChunk.flatten
            val s = if (chunk.nonEmpty) {
              Stream.eval(refTopology.get).map{topo => 
                Stream.eval(topo.random[F]).flatMap{ default => 
                Stream.emits(
                    chunk.toList.groupBy{ case (_, s, _) => // TODO Investigate Efficient Group By
                    s.flatMap(key => topo.served(HashSlot.find(key))).getOrElse(default)
                  }.toSeq
                ).evalMap{
                  case (server, rest) => 
                    keypool.map(_._1).take(server).use{m =>
                      val out = Chunk.seq(rest.map(_._3))
                      explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                        case Left(_) => m.canBeReused.set(Reusable.DontReuse)
                        case _ => Applicative[F].unit
                      }
                    }.flatMap{
                    case Right(n) => 
                      n.zipWithIndex.parTraverseN(10){
                        case (ref, i) => 
                          val v@(toSet, _, _) = rest(i)
                          ref match {
                            case Resp.Error(s) if (s.startsWith("MOVED")) => 
                              refreshTopology >> cluster.queue(Chunk.singleton(v))
                            case Resp.Error(s) if (s.startsWith("ASK")) => 
                              toSet.complete(Either.right(ref)) // TODO Handle Ask Redirection (explicit?) Send Asked and then send
                            case otherwise => 
                              toSet.complete(Either.right(otherwise))
                          }
                      }.void
                    case e@Left(_) => 
                      rest.traverse_{ case (deff, _, _) => deff.complete(e.asInstanceOf[Either[Throwable, Resp]])}
                  }

                }
              }}.parJoinUnbounded // Send All Acquired values simultaneously. 
            } else Stream.empty
            s ++ Stream.eval_(ContextShift[F].shift)
          }.parJoin(2)
            .compile
            .drain
            .background
    } yield cluster
}