package reactive.mongo;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.extract.UserTempNaming;
import de.flapdoodle.embed.process.io.directories.TempDirInPlatformTempDir;
import de.flapdoodle.embed.process.runtime.ICommandLinePostProcessor;
import de.flapdoodle.embed.process.store.ArtifactStoreBuilder;
import de.flapdoodle.embed.process.store.Downloader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static akka.testkit.SocketUtil.temporaryServerAddress;

/**
 * Created by adelegue on 15/05/2017.
 */
public interface WithMongo {

    AtomicReference<MongodProcess> mongoProcess = new AtomicReference<>();
    AtomicReference<Integer> port = new AtomicReference<>();

    static void startMongo() throws IOException {
        int port = temporaryServerAddress("localhost", false).getPort();
        MongodExecutable mongodExecutable = MongodStarter.getInstance(
                new RuntimeConfigBuilder()
                        .daemonProcess(true)
                        .artifactStore(
                                new ArtifactStoreBuilder()
                                        .executableNaming(new UserTempNaming())
                                        .tempDir(new TempDirInPlatformTempDir())
                                        .download(
                                                new DownloadConfigBuilder()
                                                        .defaultsForCommand(Command.MongoD)
                                                        .defaults()
                                                        .build()
                                        )
                                        .downloader(new Downloader())
                                        .build()
                        )
                        .processOutput(ProcessOutput.getDefaultInstanceSilent())
                        .commandLinePostProcessor(new ICommandLinePostProcessor.Noop())
                        .build()
        ).prepare(
                new MongodConfigBuilder()
                        .version(Version.V3_4_1)
                        .net(new Net(port, false))
                        .build()
        );

        WithMongo.port.set(port);
        mongoProcess.set(mongodExecutable.start());
    }

    default Integer port() {
        return port.get();
    }

    static void stopMongo() {
        mongoProcess.get().stop();
    }
}