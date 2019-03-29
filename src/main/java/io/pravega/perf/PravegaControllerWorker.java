package io.pravega.perf;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import lombok.Cleanup;

public class PravegaControllerWorker {

    private final URI controllerUri;
    private final int controllerEvents;
    private final int streamSegments;

    public PravegaControllerWorker(URI controllerUri, int controllerEvents, int streamSegments) {
        this.controllerUri = controllerUri;
        this.controllerEvents = controllerEvents;
        this.streamSegments = streamSegments;
    }

    class CreateScopesTask implements Runnable {

        private final int id;
        private final PerfStats stats;
        private final String prefix;

        CreateScopesTask(int id, PerfStats stats, String prefix) {
            this.id = id;
            this.stats = stats;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerUri);
            try {
                for (int i = 0; i < controllerEvents; i++) {
                    final Instant beginTime = Instant.now();
                    streamManager.createScope(buildName(prefix, id, controllerEvents, i));
                    stats.recordTime(null, beginTime, 0);
                    stats.print();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class DeleteScopesTask implements Runnable {

        private final int id;
        private final PerfStats stats;
        private final String prefix;

        DeleteScopesTask(int id, PerfStats stats, String prefix) {
            this.id = id;
            this.stats = stats;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerUri);
            try {
                for (int i = 0; i < controllerEvents; i++) {
                    final Instant beginTime = Instant.now();
                    streamManager.deleteScope(buildName(prefix, id, controllerEvents, i));
                    stats.recordTime(null, beginTime, 0);
                    stats.print();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class CreateStreamTask implements Runnable {

        private final int id;
        private final PerfStats stats;
        private final String prefix;

        CreateStreamTask(int id, PerfStats stats, String prefix) {
            this.id = id;
            this.stats = stats;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerUri);
            streamManager.createScope(prefix);
            try {
                for (int i = 0; i < controllerEvents; i++) {
                    final Instant beginTime = Instant.now();
                    streamManager.createStream(prefix, buildName(prefix, id, controllerEvents, i),
                            StreamConfiguration.builder()
                                               .scope(prefix)
                                               .streamName(buildName(prefix, id, controllerEvents, i))
                                               .scalingPolicy(ScalingPolicy.fixed(streamSegments))
                                               .build());
                    stats.recordTime(null, beginTime, 0);
                    stats.print();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class UpdateStreamTask implements Runnable {

        private final int id;
        private final PerfStats stats;
        private final String prefix;

        UpdateStreamTask(int id, PerfStats stats, String prefix) {
            this.id = id;
            this.stats = stats;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerUri);
            try {
                for (int i = 0; i < controllerEvents; i++) {
                    final Instant beginTime = Instant.now();
                    streamManager.updateStream(prefix, buildName(prefix, id, controllerEvents, i), StreamConfiguration.builder()
                                                                                                                      .scope(prefix)
                                                                                                                      .streamName(buildName(prefix, id, controllerEvents, i))
                                                                                                                      .scalingPolicy(ScalingPolicy.fixed(streamSegments))
                                                                                                                      .build());
                    stats.recordTime(null, beginTime, 0);
                    stats.print();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class SealStreamTask implements Runnable {

        private final int id;
        private final PerfStats stats;
        private final String prefix;

        SealStreamTask(int id, PerfStats stats, String prefix) {
            this.id = id;
            this.stats = stats;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerUri);
            try {
                for (int i = 0; i < controllerEvents; i++) {
                    final Instant beginTime = Instant.now();
                    streamManager.sealStream(prefix, buildName(prefix, id, controllerEvents, i));
                    stats.recordTime(null, beginTime, 0);
                    stats.print();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class DeleteStreamTask implements Runnable {

        private final int id;
        private final PerfStats stats;
        private final String prefix;

        DeleteStreamTask(int id, PerfStats stats, String prefix) {
            this.id = id;
            this.stats = stats;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerUri);
            try {
                for (int i = 0; i < controllerEvents; i++) {
                    final Instant beginTime = Instant.now();
                    streamManager.deleteStream(prefix, buildName(prefix, id, controllerEvents, i));
                    stats.recordTime(null, beginTime, 0);
                    stats.print();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String buildName(String prefix, int taskId, int operationCount, int operationId) {
        return prefix + "-" + (taskId * operationCount + operationId);
    }
}
