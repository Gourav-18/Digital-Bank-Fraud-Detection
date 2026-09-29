package com.bfsi.paymentservice.service;

import com.bfsi.paymentservice.dto.CreatePaymentRequest;
import com.bfsi.paymentservice.dto.PaymentOrderResponse;
import com.bfsi.paymentservice.entity.Payment;
import com.bfsi.paymentservice.entity.PaymentStatus;
import com.bfsi.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;
    @Value("${razorpay.key-secret}")
    private String keySecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    /**
     * Create Razorpay payment order.
     *
     * FLOW:
     *  1. Create order in razorpay
     *  2. Save payment record in DB
     *  3. Return order details to frontend.
     *  4. Front end shows razorpay checkout
     *  5. USer pays
     *  6. Razorpay calls webhook
     * @param request
     * @return
     */
                 /*
    The key point is: the actual customer payment is not happening anywhere in this method.

This method only creates a Razorpay Order. The actual payment happens later, usually when your frontend opens the Razorpay Checkout using the razorpayOrderId.

What this line does
Order razorpayOrder = razorpayClient.orders.create(orderRequest);

This is the important line for communicating with Razorpay.

It sends something like:

{
  "amount": 50000,
  "currency": "INR",
  "receipt": "rcpt_..."
}

to Razorpay's server.

Razorpay responds with an Order, for example:

{
  "id": "order_R9x123abc",
  "amount": 50000,
  "currency": "INR",
  "status": "created"
}

But ₹500 has NOT been paid yet.

The order is just created.

Your flow

Your current method does this:

Frontend
   │
   │ Create payment request
   ↓
Your Spring Boot Backend
   │
   │ razorpayClient.orders.create()
   ↓
Razorpay
   │
   │ Creates Order
   ↓
order_R9x123abc
   │
   ↓
Your Backend
   │
   │ Save order ID in DB
   ↓
Return PaymentOrderResponse
   │
   ↓
Frontend

Then your frontend needs to use the returned:

savedPayment.getRazorpayOrderId()

to open Razorpay Checkout.
     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order for account: {} amount: {}", request.getAccountNumber(), request.getAmount());
        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

//        Converted amount
        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();
        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "USD/INR");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace("-", "").substring(0, 10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}", razorpayOrder.get("id").toString());


        // Save payment record
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);
        return new PaymentOrderResponse(
                savedPayment.getId(),
                savedPayment.getRazorpayOrderId(),
                savedPayment.getAmount(),
                "INR",
                "CREATED",
                keyId
        );
    }

    public void handleWebhook(Map<String, Object> payload) {
        /*
        Yes. In a payment system, a webhook is a way for an external payment provider to notify your backend when something happens to a payment.

The most important distinction is:

A webhook is usually not the mechanism for the frontend to directly send events to your backend.
It is primarily a mechanism for a payment provider → backend communication.

1. What is a webhook?

Imagine your application uses a payment provider such as Stripe, Razorpay, PayPal, etc.

Your flow might look like:

Frontend
   │
   │ "I want to pay ₹500"
   ↓
Backend
   │
   │ Create payment/order
   ↓
Payment Provider
   │
   │ User completes payment
   ↓
???

The problem is:

How does your backend know that the payment actually succeeded?

The frontend shouldn't be trusted to simply say:

{
  "paymentStatus": "SUCCESS"
}

because the frontend is controlled by the user's browser.

Instead, the payment provider sends a request to a URL on your backend:

Payment Provider
       │
       │ HTTP POST
       ↓
https://api.myapp.com/payment/webhook
       │
       ↓
Backend

That HTTP request is called a webhook.

2. Example with a payment

Suppose the user buys something for ₹1,000.

Step 1 — Frontend

User clicks:

Pay ₹1,000

Frontend calls your backend:

POST /payments/create

with:

{
  "amount": 1000,
  "orderId": "ORD123"
}
Step 2 — Backend

Your backend communicates with the payment provider:

Frontend
   ↓
Your Backend
   ↓
Payment Provider

The provider creates a payment/order and gives your backend something like:

{
  "paymentId": "PAY123",
  "status": "PENDING"
}

Your backend might store:

Order ORD123
Payment PAY123
Status = PENDING
3. User completes payment

The user goes through the payment provider's UI.

Maybe they use:

UPI
Credit Card
Debit Card
Net Banking
Wallet

Eventually the payment provider knows the actual result:

PAY123
     ↓
SUCCESS

Now the provider needs to tell your backend.

4. This is where the webhook comes in

You configure a webhook URL with the payment provider:

https://api.myapp.com/api/payments/webhook

The payment provider sends:

POST /api/payments/webhook

with something like:

{
  "event": "payment.success",
  "paymentId": "PAY123",
  "orderId": "ORD123",
  "amount": 1000,
  "status": "SUCCESS"
}

Your backend receives it.

Payment Provider
       │
       │ webhook
       ↓
Payment Controller
       ↓
Payment Service
       ↓
Database
       ↓
Payment status = SUCCESS
5. Why not just use the frontend?

This is extremely important.

Suppose your frontend does:

payment.onSuccess(() => {
    fetch("/api/payment/success", {
        method: "POST"
    });
});

You cannot blindly trust this.

A malicious user could modify the browser request and send:

{
    "status": "SUCCESS"
}

without actually paying.

So the trusted flow is:

             User
               │
               ↓
          Frontend
               │
               ↓
          Your Backend
               │
               ↓
       Payment Provider
               │
        Actual payment
               │
               ↓
          Webhook
               │
               ↓
          Your Backend
               │
               ↓
          Database

The webhook gives your backend a server-to-server notification from the payment provider.
         */
        log.info("Received razorpay webhook: {}", payload.get("event"));
        String event = (String) payload.get("event");
        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        } else if ("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException(
                            "Payment not found for order: " + orderId
                    ));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", "Payment failed via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);
            log.warn("Payment failed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error Handling payment failure {}", e.getMessage());
        }

    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException(
                            "Payment not found for order: " + orderId
                    ));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
            log.info("Payment completed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error Handling payment success {}", e.getMessage());
        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");
        return (Map<String, Object>) paymentWrapper.get("entity");
    }
}
