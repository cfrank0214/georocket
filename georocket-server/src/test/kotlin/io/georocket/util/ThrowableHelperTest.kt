package io.georocket.util

import org.junit.Assert.assertEquals

import java.io.FileNotFoundException

import org.junit.Test

import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure

/**
 * Test [ThrowableHelper]
 * @author Andrej Sajenko
 */
class ThrowableHelperTest {
    /**
     * Test code of [ReplyException]
     */
    @Test
    fun testThrowableToCodeReplyException() {
        val expectedCode = 505

        val throwable = ReplyException(ReplyFailure.NO_HANDLERS, expectedCode, "Message")

        val statusCode = ThrowableHelper.throwableToCode(throwable)
        assertEquals(expectedCode.toLong(), statusCode.toLong())
    }

    /**
     * Test code of [IllegalArgumentException]
     */
    @Test
    fun testThrowableToCodeIllegalArgument() {
        val expectedCode = 400

        val throwable = IllegalArgumentException()

        val statusCode = ThrowableHelper.throwableToCode(throwable)
        assertEquals(expectedCode.toLong(), statusCode.toLong())
    }

    /**
     * Test code of [FileNotFoundException]
     */
    @Test
    fun testThrowableToCodeFileNotFound() {
        val expectedCode = 404

        val throwable = FileNotFoundException()

        val statusCode = ThrowableHelper.throwableToCode(throwable)
        assertEquals(expectedCode.toLong(), statusCode.toLong())
    }

    /**
     * Test code of unknown throwables.
     */
    @Test
    fun testThrowableToCodeThrowable() {
        val expectedCode = 500

        val throwable = Throwable()

        val statusCode = ThrowableHelper.throwableToCode(throwable)
        assertEquals(expectedCode.toLong(), statusCode.toLong())
    }

    /**
     * Test throwable message
     */
    @Test
    fun testThrowableToMessage() {
        val expectedMessage = "A Error happen!"
        val defaultMessage = "Oops!"

        val throwable = Throwable(expectedMessage)

        val message = ThrowableHelper.throwableToMessage(throwable, defaultMessage)
        assertEquals(expectedMessage, message)
    }

    /**
     * Test default message
     */
    @Test
    fun testThrowableToMessageDefault() {
        val defaultMessage = "Oops!"

        val throwable = Throwable()

        val message = ThrowableHelper.throwableToMessage(throwable, defaultMessage)
        assertEquals(defaultMessage, message)
    }
}
