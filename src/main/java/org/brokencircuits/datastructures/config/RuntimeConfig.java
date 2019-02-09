package org.brokencircuits.datastructures.config;

import org.brokencircuits.datastructures.RunOnStart;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RuntimeConfig {

  @Bean
  public CommandLineRunner runner(RunOnStart runOnStart) {
    return args -> runOnStart.run();
  }
}