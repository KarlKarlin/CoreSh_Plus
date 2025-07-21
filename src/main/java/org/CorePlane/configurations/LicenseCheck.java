package org.CorePlane.configurations;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class LicenseCheck {
    private final RestTemplate restTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final StringRedisTemplate stringRedisTemplate;

    @Value("${keygen.account.id}")
    private String KEYGEN_ACCOUNT_ID;

    private String KEYGEN_API_URL;

    @Value("${keygen.api.token}")
    private String KEYGEN_API_TOKEN;

    @Value("${keygen.product.id}")
    private String PRODUCT_ID;

    private static final String LICENSE_CACHE_PREFIX = "license:v2:";
    private static final String LICENSE_HMAC_PREFIX = "license:hmac:";

    @Value("${keygen.secret.hmac.key}")
    private String SECRET_HMAC_KEY;

    @PostConstruct
    public void init() {
        this.KEYGEN_API_URL = "https://api.keygen.sh/v1/accounts/" + KEYGEN_ACCOUNT_ID + "/licenses/actions/validate-key";
    }

    public LicenseCheck(RestTemplate restTemplate,
                        RedisTemplate<String, Object> redisTemplate,
                        StringRedisTemplate stringRedisTemplate) {
        this.restTemplate = restTemplate;
        this.redisTemplate = redisTemplate;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public boolean checkLicense(String licenseKey) {
        Boolean cachedResult = checkCachedLicense(licenseKey);
        if (cachedResult != null) {
            return cachedResult;
        }

        boolean isValid = validateWithKeygen(licenseKey);

        if (isValid) {
            cacheLicenseValidation(licenseKey, true);
        }

        return isValid;
    }

    private boolean validateWithKeygen(String licenseKey) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.parseMediaType("application/vnd.api+json")));
            headers.setContentType(MediaType.parseMediaType("application/vnd.api+json"));
            headers.set("Authorization", KEYGEN_API_TOKEN);

            Map<String, Object> requestBody = new HashMap<>();
            Map<String, Object> meta = new HashMap<>();
            meta.put("key", licenseKey);

            Map<String, String> scope = new HashMap<>();
            scope.put("product", PRODUCT_ID);
            meta.put("scope", scope);

            requestBody.put("meta", meta);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                    KEYGEN_API_URL,
                    HttpMethod.POST,
                    entity,
                    Map.class
            );

            Map<String, Object> responseBody = response.getBody();
            if (responseBody != null && responseBody.containsKey("meta")) {
                Map<String, Object> metaResponse = (Map<String, Object>) responseBody.get("meta");
                Boolean isValid = (Boolean) metaResponse.get("valid");
                String code = (String) metaResponse.get("code");
                return isValid != null && isValid && "VALID".equals(code);
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private void cacheLicenseValidation(String licenseKey, boolean isValid) {
        try {
            String timestamp = Instant.now().toString();
            String payload = String.format("%s|%s|%b", licenseKey, timestamp, isValid);
            String hmac = calculateHMAC(payload);

            String cacheKey = LICENSE_CACHE_PREFIX + licenseKey;
            Map<String, String> cacheData = new HashMap<>();
            cacheData.put("valid", String.valueOf(isValid));
            cacheData.put("timestamp", timestamp);

            redisTemplate.opsForHash().putAll(cacheKey, cacheData);
            redisTemplate.expire(cacheKey, 168, TimeUnit.HOURS);

            stringRedisTemplate.opsForValue().set(
                    LICENSE_HMAC_PREFIX + licenseKey,
                    hmac,
                    168, TimeUnit.HOURS);
        } catch (Exception e) {
            System.err.println("Failed to cache license validation: " + e.getMessage());
        }
    }

    private Boolean checkCachedLicense(String licenseKey) {
        try {
            String cacheKey = LICENSE_CACHE_PREFIX + licenseKey;
            String hmacKey = LICENSE_HMAC_PREFIX + licenseKey;

            Map<Object, Object> cacheData = redisTemplate.opsForHash().entries(cacheKey);
            String storedHmac = stringRedisTemplate.opsForValue().get(hmacKey);

            if (cacheData.isEmpty() || storedHmac == null) {
                return null;
            }

            String payload = String.format("%s|%s|%s",
                    licenseKey,
                    cacheData.get("timestamp"),
                    cacheData.get("valid"));

            if (!verifyHMAC(payload, storedHmac)) {
                invalidateCache(licenseKey);
                return null;
            }

            String timestampStr = (String) cacheData.get("timestamp");
            Instant timestamp = Instant.parse(timestampStr);
            if (Instant.now().isAfter(timestamp.plus(1, ChronoUnit.HOURS))) {
                invalidateCache(licenseKey);
                return null;
            }

            return Boolean.parseBoolean((String) cacheData.get("valid"));
        } catch (Exception e) {
            System.err.println("Failed to check cached license: " + e.getMessage());
            return null;
        }
    }

    private void invalidateCache(String licenseKey) {
        try {
            String cacheKey = LICENSE_CACHE_PREFIX + licenseKey;
            String hmacKey = LICENSE_HMAC_PREFIX + licenseKey;
            redisTemplate.delete(cacheKey);
            stringRedisTemplate.delete(hmacKey);
        } catch (Exception e) {
            System.err.println("Failed to invalidate cache: " + e.getMessage());
        }
    }

    private String calculateHMAC(String data) {
        try {
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secret_key = new SecretKeySpec(SECRET_HMAC_KEY.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            sha256_HMAC.init(secret_key);
            byte[] hash = sha256_HMAC.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC", e);
        }
    }

    private boolean verifyHMAC(String data, String hmac) {
        String calculatedHmac = calculateHMAC(data);
        return calculatedHmac.equals(hmac);
    }
}