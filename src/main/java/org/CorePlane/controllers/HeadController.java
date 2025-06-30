package org.CorePlane.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class HeadController {
    @GetMapping("/")
    public String head() {
        return "visualisation";
    }
}