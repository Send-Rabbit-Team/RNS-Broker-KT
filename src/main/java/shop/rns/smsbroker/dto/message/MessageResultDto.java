package shop.rns.smsbroker.dto.message;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import shop.rns.smsbroker.config.status.MessageStatus;

import java.time.LocalDateTime;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MessageResultDto {
    private String rMessageResultId;

    private long messageId;

    private long brokerId;

    private long contactId;

    private MessageStatus messageStatus;

    private LocalDateTime createdAt;

    private long retryCount;
}
