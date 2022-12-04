package com.example.sandboxsbnr

import com.newrelic.api.agent.*
import kotlinx.coroutines.*
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import java.net.URI
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.logging.Logger
import kotlin.coroutines.ContinuationInterceptor

@SpringBootApplication
class SandboxSbNrApplication

fun main(args: Array<String>) {
    runApplication<SandboxSbNrApplication>(*args)
}

@Controller
class HelloController(
    val logger: Logger = Logger.getLogger(HelloController::class.java.name),
    val threadPoolForP009: ExecutorService = Executors.newFixedThreadPool(2),
    val threadPoolForKP00902: ExecutorService = Executors.newFixedThreadPool(2),
) {
    @GetMapping("kp00901")
    @Trace(dispatcher = true)
    fun getKP00901(): String {
        // getKP009関数が実行されているスレッド。ThreadAと呼ぶ。
        Thread.currentThread().name = "ThreadA"
        logger.info("Running getKP009 in ${Thread.currentThread().name}")
        // TransactionはThreadA上で開始されている。
        // （getKP009関数に@Trace(dispatcher = true)アノテーションが付与されているため。）
        // transInThreadAが、それ。
        val transInThreadA = NewRelic.getAgent().transaction
        logger.info("Started transaction is ${transInThreadA.hashCode()}")

        runBlocking {
            // ここからCoroutineが始まる。
            // runBlockingのblockはThreadA上で実行されている。
            logger.info("Running runBlocking's block in ${Thread.currentThread().name}")
            // なぜThreadA上で実行されるのか？
            //   CoroutineDispatcherが、Coroutineが実行されるスレッドを決める。
            //   runBlocking関数の第一引数にCoroutineDispatcherを何も指定していない場合、
            //   親のCoroutineScopeのCoroutineDispatcherが継承される。
            //   今回の例では、親のCoroutineScopeが存在していない。
            //   その場合、BlockingEventLoopというCoroutineDispatcherが指定されるみたいである（この辺はドキュメント化されていない挙動）。
            logger.info("CoroutineDispatcher is ${this.coroutineContext[ContinuationInterceptor]}")
            //   BlockingEventLoopは、Coroutineを親と同じスレッド（ThreadA）で実行する？という挙動なのか？（この辺はドキュメント化されていない挙動）。
            val transInRunBlocking = NewRelic.getAgent().transaction

            // TransInThreadA
            // TransInRunBlocking
            // は、同じオブジェクトになっている。
            // 何故なら、両オブジェクトともにThreadA上で取得されているため。
            logger.info("Started transaction is ${transInRunBlocking.hashCode()} (== ${transInThreadA.hashCode()})")

            // 下のasyncは、ThreadA上で実行される。
            // runBlocking同様に、async関数の第一引数にCoroutineDispatcherを何も指定していない場合、親のCoroutineScopeのCoroutineDispatcherが継承されため。
            // 親のCoroutineDispatcherが、CoroutineをThreadAで実行することを規定している。
            val jobs = listOf(
                async { job1(1000L) },
                async { job1(1000L) },
                async { job1(1000L) },
            )
            jobs.awaitAll()
        }
        // 上記の一連の挙動をNewRelic上で観察してみる。
        // すると、3つのjob1がトレースされていることがわかる。
        // token.linkしてないのになぜか。
        // 理由はシンプル。全ての非同期処理がTransactionが生成されたスレッドと同じスレッド（つまりはThreadA）上で実行されているため。
        return "hello"
    }

    @GetMapping("kp00902")
    @Trace(dispatcher = true)
    fun getKP00902(): String {
        // NewRelicのTransactionは、
        // 非同期処理が、どのスレッド上で実行されているのか？を意識しないと、間違ったプログラムを書いてしまう。

        // 厄介なのは、間違った書き方をしても気付けないということ。
        // NewRelicが提供している諸々のAPI（関数）は、間違った使い方をしていたとしても、基本的には例外を投げない。
        // メトリクス取得用のプログラムなので、例外を投げないという振る舞いは正しいんだけど・・・
        // 気をつけてプログラムを書かなければ、挙動が謎の振る舞いをしているように見えてしまう。

        // 非同期処理が、どのスレッド上で実行されているのか？を常に意識した方が良い。
        // 以下にて、ハマりどころを説明。

        // getKP009関数が実行されているスレッド。ThreadAと呼ぶ。
        Thread.currentThread().name = "ThreadA"
        logger.info("Running getKP009 in ${Thread.currentThread().name}")
        // TransactionはThreadA上で開始されている。
        // （getKP009関数に@Trace(dispatcher = true)アノテーションが付与されているため。）
        val transInThreadA = NewRelic.getAgent().transaction
        logger.info("Started transaction is ${transInThreadA.hashCode()}")

        // 今回は、runBlockingのブロック（コルーチン）をスレッドプール上の任意のスレッド上で実行させるようにしてみる。
        // するとNewRelicは意味不明な挙動となる。
        runBlocking(threadPoolForKP00902.asCoroutineDispatcher()) {
            logger.info("Running runBlocking's block in ${Thread.currentThread().name}")

            // 意味不明な挙動１
            // 実は
            //   NewRelic.getAgent().transaction
            // は、変な値が入ってる。 要は、このtransaction変数は無効ということ。使えない。
            val transInRunBlocking = NewRelic.getAgent().transaction // 変な値
            // 変な値が入っている理由は、 token.linkにより、スレッドプール上のスレッドと、親のTransactionが紐付けられていないため（らしい）。
            //   https://docs.newrelic.com/docs/apm/agents/java-agent/async-instrumentation/java-agent-api-asynchronous-applications/
            // この辺・・・わかりにくいというか・・・ハマりやすい。

            // ちなみに、
            // また、transInThreadA変数も変な値が入ってる。
            logger.info("Odd transaction ${transInThreadA.hashCode()}")
            // runBlockingのblockと、getKP00902関数、この2つの処理は異なるスレッド上で実行されているため。
            // kotlinはjavaよりも非同期処理がお手軽に書けるようになっているけど、
            // 非同期処理をスレッドプール上で実行させるとなった場合は、
            // マルチスレッドプログラムを書くのと同様になるので、
            // 気をつけていないとこのような凡ミスをしてしまう。。。

            // 意味不明な挙動２
            // transInRunBlocking で何をしてもTransactionには反映されない。
            val segmentHoge = transInRunBlocking.startSegment("hoge")
            Thread.sleep(1000L)
            segmentHoge.end()

            // 意味不明な挙動３
            // 下のasyncは、スレッドプール上の任意のスレッドで実行される。
            // 従って、3つのjob1はトレースされない。
            // NewRelicの公式ドキュメントでは、「token.linkを実行し、Transactionに紐づけろ」とのこと。
            // https://docs.newrelic.com/docs/apm/agents/java-agent/async-instrumentation/java-agent-api-asynchronous-applications/
            val jobs = listOf(
                async { job1(1000L) },
                async { job1(1000L) },
                async { job1(1000L) },
            )
            jobs.awaitAll()
        }
        return "hello"
    }

    @GetMapping("kp00903")
    @Trace(dispatcher = true)
    fun getKP00903(): String {
        Thread.currentThread().name = "ThreadA"
        logger.info("Running getKP009 in ${Thread.currentThread().name}")
        val transInThreadA = NewRelic.getAgent().transaction
        logger.info("Started transaction is ${transInThreadA.hashCode()}")
        // スレッドプール上で実行される非同期処理をトレースするために
        // トークンを生成する。
        val tokenCreatedInThreadA = transInThreadA.token
        runBlocking(threadPoolForKP00902.asCoroutineDispatcher()) {
            logger.info("Running runBlocking's block in ${Thread.currentThread().name}")
            val jobs = listOf(
                async { job3(1000L, tokenCreatedInThreadA) },
                async { job3(1000L, tokenCreatedInThreadA) },
                async { job3(1000L, tokenCreatedInThreadA) },
                async { job7(1000L, tokenCreatedInThreadA) },
            )
            jobs.awaitAll()
        }
        tokenCreatedInThreadA.expire()
        return "hello"
    }

    @GetMapping("kp010")
    @Trace(dispatcher = true)
    fun getKP010(): String {
        val token = NewRelic.getAgent().transaction.token
        // Thread.sleepではなくdelay関数にするとダメ。
        // delay関数で中断している間の時間がNewRelic上では計上されない。
        // これはある意味仕方ないと言えるのかもしれない。

        // delay関数の中でtoken.linkをコールすれば良いんだと思われるが、
        // それはできない。
        // こういった中断をどう計上するかが課題だ。
        runBlocking(threadPoolForP009.asCoroutineDispatcher()) {
            val jobs = listOf(
                async { job4(1000L, token) },
                async { job4(2000L, token) },
                async { job4(1000L, token) },
            )
            jobs.awaitAll()
        }
        token.expire()
        return "hello"
    }

    @GetMapping("kp011")
    @Trace(dispatcher = true)
    fun getKP011(): String {
        val token = NewRelic.getAgent().transaction.token
        // segmentを使うことで、任意の処理単位をSegmentとしてトレースできるようになる。
        val segment = NewRelic.getAgent().transaction.startSegment("hoge")
        job6(1000L)
        runBlocking(threadPoolForP009.asCoroutineDispatcher()) {
            job5(1000L, token, segment)
        }
        token.expire()
        return "hello"
    }
}

@Trace(async = true)
suspend fun job1(wait: Long) {
    Logger.getLogger("job1").info("Running job1 in ${Thread.currentThread().name}")
    delay(wait)
}

@Trace(async = true)
fun job3(wait: Long, token: Token) {
    // 関数の最初でtoken.linkを実行！
    token.link()
    try {
        Thread.sleep(wait)
    } finally {
        // token.linkは関数の最後でも良い
        // しかし、後述する理由により、関数の最初で実行した方が良い。
        // token.link()
    }
}


@Trace(async = true)
suspend fun job4(wait: Long, token: Token) {
    try {
        delay(wait) // NewRelic上では計上されない中断（delayの中の処理まではlinkできない）
    } finally {
        token.link()
    }
}


@Trace(async = true)
suspend fun job5(wait: Long, token: Token, segment: Segment) {
    token.link()
    val segment2 = NewRelic.getAgent().transaction.startSegment("foo")
    try {
        delay(wait)
    } finally {
        segment.endAsync()
        segment2.endAsync()
    }
}

@Trace
fun job6(wait: Long) {
    val segment = NewRelic.getAgent().transaction.startSegment("seg job6")
    Thread.sleep(wait)
    segment.end()
}


@Trace(async = true)
suspend fun job7(wait: Long, token: Token) = coroutineScope {
    token.link()
    val segment = NewRelic.getAgent().transaction.startSegment("job7 segment")
    delay(wait)
    segment.end()
    val children = listOf(
        async { job701(1000L, token) },
        async { job701(1000L, token) },
        async { job701(1000L, token) },
        async { job702(1000L, token) },
        async { job703(1000L, token) },
    )
    children.awaitAll()
}


@Trace(async = true)
suspend fun job701(wait: Long, token: Token) {
    token.link()
    val segment = NewRelic.getAgent().transaction.startSegment("job701 segment")
    delay(wait)
    segment.end()
}


@Trace(async = true)
suspend fun job702(wait: Long, token: Token) = coroutineScope {
    token.link()
    val segment = NewRelic.getAgent().transaction.startSegment("job702 segment")
    segment.reportAsExternal(
        HttpParameters
            .library("dummy http client")
            .uri(URI.create("https://www.example.com/hoge/fuga"))
            .procedure("foo")
            .noInboundHeaders()
            .build()
    )
    delay(wait)
    segment.end()
}

@Trace(async = true)
suspend fun job703(wait: Long, token: Token) {
    token.link()
    val segment = NewRelic.getAgent().transaction.startSegment("job703 segment")
    coroutineScope {
        val deferredA = async {
            delay(wait)
        }
        val deferredB = async { job70301(token) }
        deferredA.await()
        deferredB.await()
        segment.end()
    }
}


@Trace(async = true)
suspend fun job70301(token: Token) = coroutineScope {
    token.link()
    val segment = NewRelic.getAgent().transaction.startSegment("job70301 segment")
    delay(100L)
    segment.end()
}