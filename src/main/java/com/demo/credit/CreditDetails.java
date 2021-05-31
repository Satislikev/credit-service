package com.demo.credit;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CreditDetails {

	private String applicationId;
	private String address;
	private Long salary;
	private boolean isInCollection;

}