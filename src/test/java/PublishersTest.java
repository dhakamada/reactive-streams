import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit test to Mono and Flux Publishers
 * Mono and Flux are implementations of the Publisher interface.
 * Flux will observer 0 to N items and eventually terminate successfully or not.
 * Mono will observe 0 or 1 item.
 * Mono<Void> hinting at most 0 items.
 * <p>
 * Reactive Streams is a specification for asynchronous stream processing.
 * Flux and Mono are implementations of the Reactive Streams Publisher interface
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
                .map(m -> m.toUpperCase()); //map() will be applied when onNext() is called

        StepVerifier.create(fluxUpperCase).expectNext("TWO").verifyComplete();
    }

    @Test
    public void backpressure() {

        final List<Integer> elements = new ArrayList<>();
//        Flux.just(1, 2, 3, 4, 5)
//                .log()
//                .subscribe(elements::add);
//        Assertions.assertThat(elements).containsExactly(1, 2, 3, 4, 5);
        
        Flux.just(1, 2, 3, 4)
                .log()
                .subscribe(new Subscriber<Integer>() {

                    private Subscription s;

                    int onNextAmount;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.s = s;
                        s.request(2);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        elements.add(integer);
                        onNextAmount++;
                        if (onNextAmount % 2 == 0) {
                            s.request(2);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });
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

    /**
     * Don't use this blocking (toIterable) operation as it will kill the Reactive pipeline
     */
    @Test
    public void block() {
        final String name = Mono.just("Diego").block();
        assertThat(name).isEqualTo("Diego");

        final Iterator<String> iterator = Flux.just("Test", "Block").toIterable().iterator();
        assertThat(iterator.next()).isEqualTo("Test");
        assertThat(iterator.next()).isEqualTo("Block");
    }

    /**
     * Create a {@link Mono} that terminates with the specified error immediately after
     */
    @Test
    public void error() {
        final Mono<String> monoError = Mono.error(new NullPointerException("NPE"));
        StepVerifier.create(monoError).expectErrorMessage("NPE").verify();
        StepVerifier.create(monoError).expectError(NullPointerException.class).verify();
    }

    /**
     * emits long values starting with 0 and incrementing at
     */
    @Test
    public void counter() {
        final Flux<Long> counterFlux = Flux.interval(Duration.ofMillis(100)).take(5);
        counterFlux.subscribe(System.out::println);
        StepVerifier.create(counterFlux).expectNext(0L, 1L, 2L, 3L, 4L).verifyComplete();
    }

    /**
     * Pick the first to emit any signal
     */
    @Test
    public void firstEmmiting() {
        final Flux<Long> delay = Flux.interval(Duration.ofSeconds(1));
        final Flux<Long> delay1 = Flux.interval(Duration.ofMillis(500));
        final Flux<String> numbersWithDelay = Flux.just("1", "2", "3", "4").zipWith(delay, (s, l) -> s);
        final Flux<String> numbersWithDelay1 = Flux.just("5", "6", "7", "8").zipWith(delay1, (s, l) -> s);
        final Flux<String> first = Flux.first(numbersWithDelay, numbersWithDelay1);
        StepVerifier.create(first).expectNext("5", "6", "7", "8").verifyComplete();

        //        Flux.defer(() -> Flux.fromIterable(Arrays.asList("Jose, Patricia"))
        //                .subscribeOn(Schedulers.elastic()));
    }

    @Test
    public void switchIfEmpty() {
        final Flux<String> fluxFallback = Helper.getFluxEmpty()
                .switchIfEmpty(Helper.getFluxFallBack());

        StepVerifier.create(fluxFallback).expectNext("Fallback").verifyComplete();
    }

    @Test
    public void fromCompletable() throws Exception {
        final CompletableFuture<Boolean> expected = CompletableFuture.completedFuture(false);

        final StringBuilder result = new StringBuilder();

        Mono.just("")
                .map(it ->
                        Mono.fromFuture(expected)
                )
                .subscribe(result::append);

        expected.get();

        Assert.assertEquals("MonoCompletionStage", result.toString());
    }

    @Test
    public void connectableFlux() {
        ConnectableFlux<Object> publish = Flux.create(fluxSink -> {
            while(true) {
                fluxSink.next(System.currentTimeMillis());
            }
        }).sample(Duration.ofSeconds(2)).publish();

        publish.subscribe(System.out::println);

        publish.connect(); //Flux will start emitting.
    }
}
