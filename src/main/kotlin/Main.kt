package org.example

import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.delay
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class HandlerImpl(
    private val client: Client,
    private val context: CoroutineContext,
) : Handler {
    override val timeout: Duration
        get() = 15L.toDuration(DurationUnit.SECONDS)

    //  private val queue = Channel<Event>()


    override fun performOperation() {
        return runBlocking {
            withContext(context) {

                val queue = flow<Event> {
                    while (isActive) {
                        val event = client.readData()
                        emit(event)
                    }
                }

                queue.collect { event ->
                    launch {
                        sendData(event)
                    }
                }
            }

        }
    }

    suspend fun sendData(event: Event) {
        val rejected = event.recipients.map { address ->
            val result = client.sendData(address, event.payload)
            Pair(result, address)
        }.filter { (result, _) ->
            result == Result.REJECTED
        }

        if (rejected.isNotEmpty()) {
            Event(rejected.map { (_, address) -> address }, event.payload)
            delay(timeout)
            sendData(event)
        }
    }
}


data class Address(val datacenter: String, val nodeId: String)
data class Event(val recipients: List<Address>, val payload: Payload)
data class Payload(val origin: String, val data: ByteArray)

enum class Result { ACCEPTED, REJECTED }

interface Client {
    //блокирующий метод для чтения данных
    fun readData(): Event

    //блокирующий метод отправки данных
    fun sendData(dest: Address, payload: Payload): Result
}

interface Handler {
    val timeout: Duration

    fun performOperation()
}