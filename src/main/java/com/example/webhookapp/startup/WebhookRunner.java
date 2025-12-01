package com.example.webhookapp.startup;

import com.example.webhookapp.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class WebhookRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(WebhookRunner.class);
    private final RestTemplate restTemplate;

    @Value("${bfhl.base-url}")
    private String baseUrl;

    @Value("${bfhl.name}")
    private String name;

    @Value("${bfhl.regNo}")
    private String regNo;

    @Value("${bfhl.email}")
    private String email;

    public WebhookRunner(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public void run(String... args) {
        String generateUrl = baseUrl + "/hiring/generateWebhook/JAVA";

        GenerateWebhookRequest request =
                new GenerateWebhookRequest(name, regNo, email);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<GenerateWebhookRequest> entity =
                new HttpEntity<>(request, headers);

        ResponseEntity<GenerateWebhookResponse> response =
                restTemplate.postForEntity(generateUrl, entity, GenerateWebhookResponse.class);

        GenerateWebhookResponse data = response.getBody();

        if (data == null) {
            log.error("No response received!");
            return;
        }

        String token = data.getAccessToken();
        String webhookUrl = data.getWebhook();

        log.info("Webhook URL = {}", webhookUrl);
        log.info("Access Token = {}", token);
        String finalSQL = decideQuery(regNo);
        sendFinalQuery(finalSQL, token, webhookUrl);
    }

    private String decideQuery(String regNo) {

        return  """
            SELECT 
                d.department_name AS DEPARTMENT_NAME,
                p.amount AS SALARY,
                CONCAT(e.first_name, ' ', e.last_name) AS EMPLOYEE_NAME,
                TIMESTAMPDIFF(YEAR, e.dob, CURDATE()) AS AGE
            FROM payments p
            JOIN employee e 
                ON p.emp_id = e.emp_id
            JOIN department d 
                ON e.department = d.department_id
            WHERE 
                DAY(p.payment_time) <> 1
            AND p.amount = (
                SELECT MAX(p2.amount)
                FROM payments p2
                JOIN employee e2 
                    ON p2.emp_id = e2.emp_id
                WHERE 
                    DAY(p2.payment_time) <> 1
                    AND e2.department = e.department
            )
            ORDER BY d.department_name;
                """;
    }

    private void sendFinalQuery(String query, String token, String webhookUrl) {

        if (webhookUrl == null || webhookUrl.isEmpty()) {
            webhookUrl = baseUrl + "/hiring/testWebhook/JAVA";
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", token);

        FinalQueryRequest request = new FinalQueryRequest(query);

        HttpEntity<FinalQueryRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response =
                restTemplate.postForEntity(webhookUrl, entity, String.class);

        log.info("Final submission response: {}", response.getBody());
    }
}
