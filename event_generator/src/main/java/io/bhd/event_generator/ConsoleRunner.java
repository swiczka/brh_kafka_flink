package io.bhd.event_generator;

import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/** Prosta konsola: komendy wpisywane w terminalu sterują generatorem. */
@Component
public class ConsoleRunner implements CommandLineRunner {

    private static final String HELP = """
            Komendy:
              start | stop              - włącz / zatrzymaj generowanie
              rate <n>                  - tempo w zdarzeniach na sekundę
              tamper|duplicate|gap|garbage <0-100>
                                        - procent danej anomalii
              status                    - ustawienia i liczniki
              forge                     - test ACL: zapis prosto do events.verified
              stats                     - liczba wiadomości w topikach (zatwierdzone / wszystkie)
              complex-stats             - liczba wiadomości z podziałem na commited/uncommited
              help | quit""";

    private final EventGenerator generator;
    private final KafkaTemplate<String, String> kafka;
    private final Stats stats;

    public ConsoleRunner(EventGenerator generator, KafkaTemplate<String, String> kafka, Stats stats) {
        this.generator = generator;
        this.kafka = kafka;
        this.stats = stats;
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println(HELP);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            String[] cmd = line.trim().toLowerCase().split("\\s+");
            try {
                switch (cmd[0]) {
                    case "" -> { }
                    case "start" -> generator.start();
                    case "stop" -> generator.stop();
                    case "rate" -> generator.setRate(number(cmd));
                    case "tamper", "duplicate", "gap", "garbage" -> generator.setPercent(cmd[0], number(cmd));
                    case "status" -> System.out.println(generator.status());
                    case "forge" -> System.out.println(generator.forge());
                    case "stats" -> System.out.println(stats.report());
                    case "complex-stats" -> System.out.println(generator.complexStats());
                    case "help" -> System.out.println(HELP);
                    case "quit", "exit" -> {
                        generator.stop();
                        kafka.flush();
                        return;
                    }
                    default -> System.out.println("Nieznana komenda, wpisz help");
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Błąd: " + e.getMessage());
            }
        }
    }

    private static int number(String[] cmd) {
        if (cmd.length < 2) {
            throw new IllegalArgumentException("brak liczby, np. " + cmd[0] + " 10");
        }
        return Integer.parseInt(cmd[1]); // NumberFormatException dziedziczy po IllegalArgumentException
    }
}
