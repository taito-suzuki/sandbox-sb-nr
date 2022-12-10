package com.example.sandboxsbnr

import com.newrelic.api.agent.NewRelic
import com.newrelic.api.agent.Segment
import com.newrelic.api.agent.Trace
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async

@Trace(async = true)
fun <T> CoroutineScope.asyncw(
    block: suspend CoroutineScope.() -> T,
): Deferred<T> {
    this.coroutineContext[NewRelicTokenContext]?.token?.link()
    val parts = block.javaClass.typeName.split("$")
    val classFullName = parts[0]
    val className = classFullName.split(".").last()
    val methodName = if (parts.size > 1) {
        parts[1]
    } else {
        ""
    }
    val stackTrace = Throwable().stackTrace[1]
    val segmentName2 = "${className}.${methodName}(${stackTrace.fileName}:${stackTrace.lineNumber})"
    val transaction = NewRelic.getAgent().transaction
    transaction.tracedMethod.setMetricName("nr_async", segmentName2)
    val segment = transaction.startSegment("(${stackTrace.fileName}:${stackTrace.lineNumber})")
    val v = async(
        context = this.coroutineContext,
        block = block,
    )
    v.invokeOnCompletion {
        segment.end()
    }
    return v
}

fun nrSegment(): Segment {
    val stackTrace = Throwable().stackTrace[1]
    return NewRelic.getAgent().transaction.startSegment("nr_segment", "${stackTrace.fileName}:${stackTrace.lineNumber}")
}