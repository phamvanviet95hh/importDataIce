package org.example;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Facility {
    public String stt;
    public String ma_cskb;
    public String ten_cskb;
    public String rank_cskb;
    public String dia_chi;
    public String tuyen_cskb;
}
