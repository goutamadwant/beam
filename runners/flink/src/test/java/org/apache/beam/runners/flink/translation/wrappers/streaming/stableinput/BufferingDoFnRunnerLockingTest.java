/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.utils.Locker;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.joda.time.Instant;
import org.junit.Test;
import org.mockito.Mockito;

/** Tests locking of state transitions in {@link BufferingDoFnRunner}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class BufferingDoFnRunnerLockingTest {

  @Test
  public void testStateTransitionsUseLocker() throws Exception {
    ReentrantLock stateLock = new ReentrantLock();
    AtomicReference<CountDownLatch> lockAttempt = new AtomicReference<>();
    Supplier<Locker> locker =
        () -> {
          lockAttempt.get().countDown();
          return Locker.locked(stateLock);
        };
    BufferingDoFnRunner runner = createBufferingDoFnRunner(locker);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    try {
      Future<?> timer;
      stateLock.lock();
      try {
        lockAttempt.set(new CountDownLatch(1));
        timer =
            executor.submit(
                () ->
                    runner.onTimer(
                        "timer",
                        "family",
                        "key",
                        GlobalWindow.INSTANCE,
                        new Instant(40),
                        new Instant(5),
                        TimeDomain.EVENT_TIME,
                        CausedByDrain.NORMAL));
        assertTrue(lockAttempt.get().await(5, TimeUnit.SECONDS));
        assertFalse(timer.isDone());
        assertThat(runner.getOutputWatermarkHold(), is(Long.MAX_VALUE));
      } finally {
        stateLock.unlock();
      }
      timer.get(5, TimeUnit.SECONDS);
      assertThat(runner.getOutputWatermarkHold(), is(5L));

      Future<?> processElement;
      stateLock.lock();
      try {
        lockAttempt.set(new CountDownLatch(1));
        processElement =
            executor.submit(
                () ->
                    runner.processElement(
                        WindowedValues.timestampedValueInGlobalWindow("element", new Instant(10))));
        assertTrue(lockAttempt.get().await(5, TimeUnit.SECONDS));
        assertFalse(processElement.isDone());
        assertThat(runner.getOutputWatermarkHold(), is(5L));
      } finally {
        stateLock.unlock();
      }
      processElement.get(5, TimeUnit.SECONDS);
      assertThat(runner.getOutputWatermarkHold(), is(5L));

      Future<?> checkpoint;
      stateLock.lock();
      try {
        lockAttempt.set(new CountDownLatch(1));
        checkpoint =
            executor.submit(
                () -> {
                  runner.checkpoint(1L);
                  return null;
                });
        assertTrue(lockAttempt.get().await(5, TimeUnit.SECONDS));
        assertFalse(checkpoint.isDone());
        assertThat(runner.currentStateIndex, is(0));
      } finally {
        stateLock.unlock();
      }
      checkpoint.get(5, TimeUnit.SECONDS);
      assertThat(runner.currentStateIndex, is(1));

      Future<?> checkpointCompleted;
      stateLock.lock();
      try {
        lockAttempt.set(new CountDownLatch(1));
        checkpointCompleted =
            executor.submit(
                () -> {
                  runner.checkpointCompleted(1L);
                  return null;
                });
        assertTrue(lockAttempt.get().await(5, TimeUnit.SECONDS));
        assertFalse(checkpointCompleted.isDone());
        assertThat(runner.getOutputWatermarkHold(), is(5L));
      } finally {
        stateLock.unlock();
      }
      checkpointCompleted.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private static BufferingDoFnRunner createBufferingDoFnRunner(Supplier<Locker> locker)
      throws Exception {
    DoFnRunner doFnRunner = Mockito.mock(DoFnRunner.class);
    OperatorStateBackend operatorStateBackend = Mockito.mock(OperatorStateBackend.class);

    ListState checkpointListState = Mockito.mock(ListState.class);
    ListState timestampHoldListState = Mockito.mock(ListState.class);
    Mockito.when(operatorStateBackend.getUnionListState(Mockito.<ListStateDescriptor>any()))
        .thenAnswer(
            invocation ->
                ((ListStateDescriptor) invocation.getArgument(0))
                        .getName()
                        .equals("notYetAcknowledgedSnapshots")
                    ? checkpointListState
                    : timestampHoldListState);
    Mockito.when(checkpointListState.get()).thenReturn(Collections.emptyList());
    Mockito.when(timestampHoldListState.get()).thenReturn(Collections.emptyList());

    ListState bufferListState = Mockito.mock(ListState.class);
    Mockito.when(bufferListState.get()).thenReturn(Collections.emptyList());
    Mockito.when(operatorStateBackend.getListState(Mockito.<ListStateDescriptor>any()))
        .thenReturn(bufferListState);

    return BufferingDoFnRunner.create(
        doFnRunner,
        "stable-input",
        StringUtf8Coder.of(),
        WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
        operatorStateBackend,
        null,
        1,
        new SerializablePipelineOptions(FlinkPipelineOptions.defaults()),
        locker,
        null,
        null);
  }
}
