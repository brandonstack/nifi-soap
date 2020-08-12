/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.soap;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.client.Options;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.http.impl.httpclient3.HttpTransportPropertiesImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@SupportsBatching
//@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"SOAP", "Get", "Ingest", "Ingress"})
@CapabilityDescription("Execute provided request against the SOAP endpoint. The result will be left in it's orginal form. " +
        "This processor can be scheduled to run on a timer, or cron expression, using the standard scheduling methods, " +
        "or it can be triggered by an incoming FlowFile. If it is triggered by an incoming FlowFile, then attributes of " +
        "that FlowFile will be available when evaluating the executing the SOAP request.")
@WritesAttribute(attribute = "mime.type", description = "Sets mime type to text/xml")
@DynamicProperty(name = "The name of a input parameter the needs to be passed to the SOAP method being invoked.",
        value = "The value for this parameter '=' and ',' are not considered valid values and must be escpaed . Note, if the value of parameter needs to be an array the format should be key1=value1,key2=value2.  ",
        description = "The name provided will be the name sent in the SOAP method, therefore please make sure " +
                "it matches the wsdl documentation for the SOAP service being called. In the case of arrays " +
                "the name will be the name of the array and the key's specified in the value will be the element " +
                "names pased.")

public class GetSOAP extends AbstractProcessor {


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("A Response FlowFile will be routed upon success.")
            .build();
    /**
     * relation of failure
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The original FlowFile will be route on any type of connection failure, timeout or general exception.")
            .build();


    protected static final PropertyDescriptor ENDPOINT_URL = new PropertyDescriptor
            .Builder()
            .name("Endpoint URL")
            .description("The endpoint url that hosts the web service(s) that should be called.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    protected static final PropertyDescriptor NAMESPACE_URL = new PropertyDescriptor
            .Builder()
            .name("Namespace URL")
            .description("the namespace URI.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    protected static final PropertyDescriptor METHOD_NAME = new PropertyDescriptor
            .Builder()
            .name("SOAP Method Name")
            .description("The method exposed by the SOAP webservice that should be invoked.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor USER_NAME = new PropertyDescriptor
            .Builder()
            .name("User name")
            .sensitive(true)
            .description("The username to use in the case of basic Auth")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder()
            .name("Password")
            .sensitive(true)
            .description("The password to use in the case of basic Auth")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor USER_AGENT = new PropertyDescriptor
            .Builder()
            .name("User Agent")
            .defaultValue("NiFi SOAP Processor")
            .description("The user agent string to use, the default is Nifi SOAP Processor")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor SO_TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Socket Timeout")
            .defaultValue("60000")
            .description("The timeout value to use waiting for data from the webservice")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    protected static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Connection Timeout")
            .defaultValue("60000")
            .description("The timeout value to use waiting to establish a connection to the web service")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    protected static final PropertyDescriptor CONTENT_PARAMETER_KEY = new PropertyDescriptor
            .Builder()
            .name("Method Parameter of Content")
            .description("Read Flowfile content and set as Request Parameter")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private List<PropertyDescriptor> descriptors;
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();
    private ServiceClient serviceClient;

    private static boolean isHTTPS(final String url) {
        return url.charAt(4) == ':';
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> myDescriptors = new ArrayList<>();
        myDescriptors.add(ENDPOINT_URL);
        myDescriptors.add(NAMESPACE_URL);
        myDescriptors.add(METHOD_NAME);
        myDescriptors.add(USER_NAME);
        myDescriptors.add(PASSWORD);
        myDescriptors.add(USER_AGENT);
        myDescriptors.add(SO_TIMEOUT);
        myDescriptors.add(CONNECTION_TIMEOUT);
        myDescriptors.add(CONTENT_PARAMETER_KEY);
        this.descriptors = Collections.unmodifiableList(myDescriptors);

    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        } else {
            super.onPropertyModified(descriptor, oldValue, newValue);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(2);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the method name and parameter names and values for '" + propertyDescriptorName + "' the SOAP method being called.")
                .expressionLanguageSupported(true)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        Options options = new Options();

        final String endpointURL = context.getProperty(ENDPOINT_URL).getValue();
        options.setTo(new EndpointReference(endpointURL));

        if (isHTTPS(endpointURL)) {
            options.setTransportInProtocol(Constants.TRANSPORT_HTTPS);
        } else {
            options.setTransportInProtocol(Constants.TRANSPORT_HTTP);
        }

        options.setCallTransportCleanup(true);
        options.setProperty(HTTPConstants.CHUNKED, false);


        options.setProperty(HTTPConstants.USER_AGENT, context.getProperty(USER_AGENT).getValue());
        options.setProperty(HTTPConstants.SO_TIMEOUT, context.getProperty(SO_TIMEOUT).asInteger());
        options.setProperty(HTTPConstants.CONNECTION_TIMEOUT, context.getProperty(CONNECTION_TIMEOUT).asInteger());
        //get the username and password -- they both must be populated.

        final String userName = context.getProperty(USER_NAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        if (null != userName && null != password && !userName.isEmpty() && !password.isEmpty()) {

            HttpTransportPropertiesImpl.Authenticator
                    auth = new HttpTransportPropertiesImpl.Authenticator();
            auth.setUsername(userName);
            auth.setPassword(password);
            options.setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
        }
        try {
            serviceClient = new ServiceClient();
            serviceClient.setOptions(options);
        } catch (AxisFault axisFault) {
            getLogger().error("Failed to create webservice client, please check that the service endpoint is available and " +
                    "the property is valid.", axisFault);
            throw new ProcessException(axisFault);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        try {
            serviceClient.cleanup();
        } catch (AxisFault axisFault) {
            getLogger().error("Failed to clean up the web service client.", axisFault);
            throw new ProcessException(axisFault);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        //get the dynamic properties, execute the call and return the results
        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace omNamespace = fac.createOMNamespace(context.getProperty(NAMESPACE_URL).getValue(), "nifi");

        final OMElement method = getSoapMethod(fac, omNamespace, context.getProperty(METHOD_NAME).getValue());

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }
        //now we need to walk the arguments and add them
        addArgumentsToMethod(context, fac, omNamespace, method, flowFile, session);
        // execute soap method
        flowFile = executeSoapMethod(method, session, flowFile);
        if (flowFile != null) {
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private String getContent(ProcessSession session, FlowFile flowFile) {
        final AtomicReference<String> value = new AtomicReference<>();
        flowFile.getSize();
        session.read(flowFile, in -> {
            final String content = IOUtils.toString(in, Charset.defaultCharset());
            value.set(content);
        });
        return value.get();
    }

    FlowFile executeSoapMethod(OMElement method, ProcessSession session, FlowFile flowFile) {
        try {
            // send request and get response
            OMElement result = serviceClient.sendReceive(method);
            // get resp text
            String response = result.getFirstElement().getText();
            // write to new FlowFile
            FlowFile respFlowFile = session.write(flowFile, out -> out.write(response.getBytes()));

            // set attributes
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/xml");
            return session.putAllAttributes(respFlowFile, attributes);
        } catch (AxisFault axisFault) {
            final ComponentLog logger = getLogger();
            if (null != logger) {
                logger.error("Failed invoking the web service method", axisFault);
            }
            session.transfer(flowFile, REL_FAILURE);
            return null;
        }
    }

    void addArgumentsToMethod(ProcessContext context, OMFactory fac, OMNamespace omNamespace, OMElement method, FlowFile flowFile, ProcessSession session) {
        final String contentParam = context.getProperty(CONTENT_PARAMETER_KEY).getValue();
        if (StringUtils.isNotEmpty(contentParam)) {
            String dynamicValue = getContent(session, flowFile);
            addParamToMethod(fac, omNamespace, method, contentParam, dynamicValue);
        }
        for (String dynamicKey : dynamicPropertyNames) {
            String dynamicValue = context.getProperty(dynamicKey).evaluateAttributeExpressions(flowFile).getValue();
            addParamToMethod(fac, omNamespace, method, dynamicKey, dynamicValue);
        }
    }

    private void addParamToMethod(OMFactory fac, OMNamespace omNamespace, OMElement method, String dynamicKey, String dynamicValue) {
        OMElement value = getSoapMethod(fac, omNamespace, dynamicKey);
        value.addChild(fac.createOMText(value, dynamicValue));
        method.addChild(value);
    }

    OMElement getSoapMethod(OMFactory fac, OMNamespace omNamespace, String value) {
        return fac.createOMElement(value, omNamespace);
    }
}
