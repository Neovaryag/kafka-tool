package com.neovaryag.kafkatool;

import com.neovaryag.kafkatool.ui.JFrame;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class KafkaToolApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(KafkaToolApplication.class, JFrame.class).headless(false).run(args);
    }

}
