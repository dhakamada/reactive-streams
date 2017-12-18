import reactor.core.publisher.Flux;

/**
 * @author dhakamada
 * @version $Revision: $<br/>
 * $Id: $
 * @since 12/18/17 2:14 PM
 */
public class Helper {

    public static Flux<String> getFluxEmpty() {
        return Flux.empty();
    }

    public static Flux<String> getFluxFallBack() {
        return Flux.just("Fallback");
    }
}
