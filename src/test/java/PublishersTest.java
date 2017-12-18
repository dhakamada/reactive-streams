import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit test to Mono and Flux Publishers
 * Mono and Flux are implementations of the Publisher interface.
 * Flux will observer 0 to N items and eventually terminate successfully or not.
 * Mono will observe 0 or 1 item.
 * Mono<Void> hinting at most 0 items.
 *
 * @author dhakamada
 */
@RunWith(MockitoJUnitRunner.class)
public class PublishersTest {

    @Test
    public void eventEmpty() {
        final Mono<String> emptyMono = Mono.empty();
        //StepVerifier - to verify async process
        StepVerifier.create(emptyMono).verifyComplete();

        final Flux<String> emptyFlux = Flux.empty();
        StepVerifier.create(emptyFlux).verifyComplete();
    }

    @Test
    public void notEmpty() {
        final Mono<String> monoJust = Mono.just("Diego");
        StepVerifier.create(monoJust).expectNext("Diego").verifyComplete();

        final Flux<String> fluxJust = Flux.just("Joao", "Maria");
        StepVerifier.create(fluxJust).expectNext("Joao", "Maria").verifyComplete();

        final String wordArray = "A B C";
        final Flux<String> fluxArray = Flux.fromArray(wordArray.split(" "));
        StepVerifier.create(fluxArray).expectNext("A", "B", "C").verifyComplete();

        final Flux<String> fluxInterable = Flux.fromIterable(Arrays.asList("Jose, Patricia"));
        StepVerifier.create(fluxInterable).expectNext("Jose", "Patricia").verifyComplete();
    }

    @Test
    public void withOperationStreams() {
        final Mono<String> monoLowweCase = Mono.just("One").map(m -> m.toLowerCase());
        StepVerifier.create(monoLowweCase).expectNext("one").verifyComplete();

        final Flux<String> fluxUpperCase = Flux.just("Two", "Four")
                .filter(s -> s.startsWith("T"))
                .map(m -> m.toUpperCase());

        StepVerifier.create(fluxUpperCase).expectNext("TWO").verifyComplete();
    }

    @Test
    public void zip() {
        final Flux<String> f1 = Flux.just("Reactive", "Spring");
        final Flux<String> f2 = Flux.just("Programming", "Reactor");

        final Flux<String> map = Flux.zip(f1, f2)
                .map(t -> t.getT1() + " " + t.getT2());

        StepVerifier.create(map).expectNext("Reactive Programming", "Spring Reactor").verifyComplete();

        //with delay
        final Flux<Long> delay = Flux.interval(Duration.ofMillis(1000));
        final Flux<String> f1WithDelay = f1.zipWith(delay, (s, l) -> s);
        final Flux<String> mapWithDelay = Flux.zip(f1WithDelay, f2)
                .map(t -> t.getT1() + " " + t.getT2());

        StepVerifier.create(mapWithDelay).expectNext("Reactive Programming", "Spring Reactor").verifyComplete();
    }

    /**
     * merge can interleave the outputs,
     * while concat will first wait for earlier streams to finish before processing later streams
     */
    @Test
    public void mergeAndConcat() {

        final Flux<String> numbers1 = Flux.just("7", "8", "9");
        final Flux<String> numbers2 = Flux.just("1", "2", "3", "4", "5", "6");

        final Flux<String> eventMerge = numbers2.mergeWith(numbers1);
        StepVerifier.create(eventMerge).expectNext("1", "2", "3", "4", "5", "6", "7", "8", "9").verifyComplete();

        final Flux<Long> delay = Flux.interval(Duration.ofSeconds(1));
        final Flux<String> numbersWithDelay = Flux.just("1", "2", "3", "4").zipWith(delay, (s, l) -> s);
        final Flux<String> numbersWithDelay1 = Flux.just("5", "6", "7", "8").zipWith(delay, (s, l) -> s);

        final Flux<String> mergeWithDelaFlux = numbersWithDelay.mergeWith(numbersWithDelay1);
//        StepVerifier.create(mergeWithDelaFlux).expectNext("1", "5", "2", "6", "3", "7", "4", "8").verifyComplete();
//         or
//        StepVerifier.create(mergeWithDelaFlux).expectNext("5","1", "6", "2", "7", "3", "8", "4").verifyComplete();

        final Flux<String> concatWithDelayFlux = numbersWithDelay.concatWith(numbersWithDelay1);
        StepVerifier.create(concatWithDelayFlux).expectNext("1", "2", "3", "4", "5", "6", "7", "8");
    }

    @Test
    public void block() {
        final String name = Mono.just("Diego").block();
        assertThat(name).isEqualTo("Diego");

        final Iterator<String> iterator = Flux.just( "Test", "Block").toIterable().iterator();
        assertThat(iterator.next()).isEqualTo("Test");
        assertThat(iterator.next()).isEqualTo("Block");
    }
}
