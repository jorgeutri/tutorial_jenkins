package language.detector.examples.processors;

import com.google.common.base.Optional;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 *
 * @author jorge
 */
@SideEffectFree
@Tags({ "LANGUAGE_DETECTOR" })
@CapabilityDescription("Fetch value from json path.")
@InputRequirement(value = Requirement.INPUT_REQUIRED)
public class LanguageDetectorMain extends AbstractProcessor {

	/* Atributos detector_language */
	private List<LanguageProfile> languageProfiles;
	private LanguageDetector languageDetector;
	private TextObjectFactory textObjectFactory;
	private TextObject textObject;
	private Optional<LdLocale> lang;

	/* Atributos NIFI */
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;

	/* Properties */
	public static final PropertyDescriptor ALPHA = new PropertyDescriptor.Builder().name("Prob detector").required(true)
			.defaultValue("0.5").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	/* Relationship */
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
			.description("Succes relationship").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("FAILURE")
			.description("Fail relationship").build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		/* Carga properties */
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(ALPHA);
		this.properties = Collections.unmodifiableList(properties);

		/* Carga Relationship */
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final ComponentLog logger = getLogger();
		final AtomicReference<String> value = new AtomicReference<>();
		final AtomicReference<Throwable> error = new AtomicReference<>(null);

		/* crear un nuevo archivo de flujo */
		// FlowFile flowfile = session.create();

		FlowFile flowfile = session.get();

		if (flowfile == null) {
			logger.error("Flow File nulo");
			return;
		}
		/* lectura del contenido del flujo de entrada */
		session.read(flowfile, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
				try {
					String texto = "";
					InputStreamReader isr = null;
					BufferedReader br = null;
					try {
						/* Texto de entrada */
						isr = new InputStreamReader(in, "utf8");
						br = new BufferedReader(isr);
						String linea;
						while ((linea = br.readLine()) != null) {
							texto += linea;
						}
					} catch (Exception ex) {
						logger.error("Error ", ex);
						error.set(ex);

					}
					/* Procesar texto de entrada */
					value.set(detector(texto));

				} catch (Exception ex) {
					ex.printStackTrace();
					getLogger().error("Failed to read string.");
				}
			}
		});

		// Escritura de los resultados en los atributos
		String results = value.get();
		
		//FlowFile clon = session.clone(flowfile);
		try {
			flowfile = session.putAttribute(flowfile, "Language", results);
		} catch (Exception e) {
			logger.error("Error ", e);
			error.set(e);
		}
		
		if (error.get() != null) {
			logger.error("error al procesar el fichero el fichero",
					new Object[] { flowfile, error.get() });
			session.transfer(flowfile, REL_FAILURE);

		} else {
			session.transfer(flowfile, SUCCESS);
		}
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	public String detector(String textInt) throws Exception {
		// Carga los lenguajes disponibles
		this.languageProfiles = new LanguageProfileReader().readAllBuiltIn();

		// build language detector:
		this.languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
				.withProfiles(languageProfiles).build();

		// create a text object factory
		this.textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

		// Crea el objeto texto que se procesara para la deteccion de idioma
		this.textObject = textObjectFactory.forText(textInt.toString());

		// detecta el idioma del texto
		this.lang = languageDetector.detect(textObject, Double.valueOf(ALPHA.getDefaultValue()));

		return lang.get().toString();
	}
}
