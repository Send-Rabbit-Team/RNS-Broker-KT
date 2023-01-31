package shop.rns.smsbroker.dto.sms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import shop.rns.smsbroker.config.status.MessageStatus;
import shop.rns.smsbroker.config.type.MessageType;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SmsMessageDto {
    private long messageId;

    private String from;

    @JsonIgnore
    private String to;

    private String subject;

    private String content;

    private List<String> images;

    private MessageStatus messageStatus;

    private MessageType messageType;

    @JsonIgnore
    private String reserveTime;

    @JsonIgnore
    private String scheduleCode;
}
