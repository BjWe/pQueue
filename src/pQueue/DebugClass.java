package pQueue;

import de.starface.core.component.StarfaceComponent;
import de.starface.core.component.StarfaceComponentProvider;
import de.vertico.starface.module.core.model.Visibility;
import de.vertico.starface.module.core.runtime.IAGIJavaExecutable;
import de.vertico.starface.module.core.runtime.IAGIRuntimeEnvironment;
import de.vertico.starface.module.core.runtime.annotations.Function;

import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.Map;

@Function(visibility= Visibility.Private, rookieFunction=true, description="Default")
public class DebugClass implements IAGIJavaExecutable {

    @Override
    public void execute(IAGIRuntimeEnvironment context) throws Exception {

        Logger log = context.getLog();

        StarfaceComponentProvider componentProvider = context.provider();
        Field privateField = StarfaceComponentProvider.class.getDeclaredField("components");
        privateField.setAccessible(true);

        Map<Class<? extends StarfaceComponent>, StarfaceComponent> components = (Map<Class<? extends StarfaceComponent>, StarfaceComponent>) privateField.get(componentProvider);

        for (Map.Entry<Class<? extends StarfaceComponent>, StarfaceComponent> entry : components.entrySet()) {
            log.info(entry.getKey().getName() + " -> " + entry.getValue().getClass().getName());
        }

    }
}
