package com.example.sandboxsbnr

import com.newrelic.api.agent.NewRelic
import com.newrelic.api.agent.Segment
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class DefferedW<T>(
    private val v: Deferred<T>,
    private val segment: Segment,
) {
    private var completed: Boolean = false
    suspend fun await(): T {
        val r = v.await()
        if (!completed) {
            segment.end()
            completed = true
        }
        return r
    }
}

class DeferredWW<T>(
    private val v: Deferred<T>,
    private val segment: Segment,
) : Deferred<T> by v {
    private var completed = false
    override suspend fun await(): T {
        println("await!")
        val returned = this.await()
        if (!completed) {
            segment.end()
            completed = true
        }
        return returned
    }
}

fun <T> CoroutineScope.asyncw(
    context: CoroutineContext = EmptyCoroutineContext,
    start: CoroutineStart = CoroutineStart.DEFAULT,
    block: suspend CoroutineScope.() -> T,
): Deferred<T> {
    val stackTraces = Throwable().stackTrace.filterIndexed { i, _ ->
        i >= 2
    }
    val stackTrace = stackTraces.first()
    val segment = NewRelic.getAgent().transaction.startSegment(stackTrace.toString())
    val v = async(
        context = context,
        start = start,
        block = block,
    )
    v.invokeOnCompletion {
        println("aaaaaaa")
        segment.end()
    }
    return v
}
